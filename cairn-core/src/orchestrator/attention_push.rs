//! Attention push-queue operations (CAIRN-1880, slice A of the attention/wake
//! rebuild — see `docs/attention-redesign.md`).
//!
//! One delivery queue per agent. Everything that reaches an agent from *outside
//! its own turn* is a **push**: a row in `attention_pushes` carrying a
//! `recipient` (the watcher job), a `content_ref` (the URI of the underlying
//! resolvable thing), a `wake` level (`passive`/`wake`/`interrupt`), a
//! `boundary` (`event`/`turn`), and a `key` for supersession.
//!
//! Supersession is by `(recipient, key)` among *undelivered* rows: a newer push
//! with the same key replaces an older undelivered one in place ([`push`]). A
//! push is **delivered** the instant a durable event carries it, recorded by
//! stamping `delivered_event_id` ([`stamp_delivered`], first-writer-wins under
//! the NULL guard). A delivered row leaves the partial unique index, so a later
//! same-key push starts a fresh row.
//!
//! At drain a push is skipped if its referent already resolved
//! ([`lazy_resolve_live`]): the **key prefix** selects which referent table to
//! check (`review:`/`question:`/`permission:`), reusing the same resolution
//! columns the legacy delivery path checks. `catchup:`/`direct:`/`resolved:` and
//! any other prefix are informational and never skip.
//!
//! This is the pure substrate (slice A). There are no callers yet — creators
//! and delivery sites land in later slices — so the public API is allowed to be
//! dead code crate-lint-wise rather than wiring half a delivery path early.

#![allow(dead_code)]

use std::collections::{HashMap, HashSet};

use cairn_common::uri::{parse_uri, CairnResource};
use cairn_db::turso::{params, Value};
use uuid::Uuid;

use crate::storage::{DbError, DbResult, LocalDb, RowExt};

/// Wake level: where a push sits on the wake axis.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wake {
    /// Never wakes an idle agent. On an active agent it still rides along at
    /// its `boundary` like any other push — wake level governs idle-waking, not
    /// whether a running agent sees the push.
    Passive,
    /// Wakes an idle agent; on an active agent lands at its `boundary`.
    Wake,
    /// Breaks the running turn now.
    Interrupt,
}

/// Build the durable wake-card payload from the owning database. Question and
/// permission push keys carry their canonical ask URI, which remains precise
/// even though `content_ref` intentionally points at the issue-level context.
/// Looking up receipts here preserves an answer that races with delivery after
/// the push was selected but before its carrying event was rendered.
pub async fn push_event_content_json_with_resolutions(
    db: &LocalDb,
    pushes: &[Push],
    resolved: &str,
) -> DbResult<String> {
    let mut resolutions = std::collections::HashMap::new();
    for push in pushes {
        let Some((kind, ask_ref)) = push.key.split_once(':') else {
            continue;
        };
        if !matches!(kind, "question" | "permission") {
            continue;
        }
        if let Some(receipt) = resolution_receipt_for_ask(db, ask_ref).await? {
            resolutions.insert(ask_ref.to_string(), receipt);
        }
    }

    let mut value: serde_json::Value = serde_json::from_str(
        &pushes_to_briefing_json_with_resolutions(pushes, &resolutions),
    )
    .unwrap_or_else(|_| serde_json::json!({ "active": [], "catchup": [] }));
    if let Some(object) = value.as_object_mut() {
        object.insert(
            "resolved".into(),
            serde_json::Value::String(resolved.into()),
        );
    }
    Ok(value.to_string())
}

async fn resolution_receipt_for_ask(
    db: &LocalDb,
    ask_ref: &str,
) -> DbResult<Option<cairn_db::models::ResolutionReceipt>> {
    let Some(resource) = parse_uri(ask_ref) else {
        return Ok(None);
    };
    let (table, project, number, exec_seq, node, task, segment) = match resource {
        CairnResource::NodeQuestion {
            project,
            number,
            exec_seq,
            node_id,
            segment,
        } => ("prompts", project, number, exec_seq, node_id, None, segment),
        CairnResource::NodePermission {
            project,
            number,
            exec_seq,
            node_id,
            segment,
        } => (
            "permission_requests",
            project,
            number,
            exec_seq,
            node_id,
            None,
            segment,
        ),
        CairnResource::TaskPermission {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
            segment,
        } => (
            "permission_requests",
            project,
            number,
            exec_seq,
            node_id,
            Some(task_name),
            segment,
        ),
        _ => return Ok(None),
    };
    db.read(|conn| {
        Box::pin(async move {
            let target_node = task.as_deref().unwrap_or(&node);
            let parent_node = task.as_ref().map(|_| node.as_str());
            let sql = format!(
                "SELECT a.resolution_id, a.resolution_surface, a.resolution_provider, \
                    a.resolution_conversation, a.resolution_actor, \
                    COALESCE(a.{resolved_column}, a.created_at) \
             FROM {table} a \
             JOIN runs r ON r.id=a.run_id \
             JOIN jobs j ON j.id=COALESCE(a.job_id, r.job_id) \
             JOIN executions e ON e.id=j.execution_id \
             JOIN issues i ON i.id=COALESCE(j.issue_id, r.issue_id) \
             JOIN projects p ON p.id=i.project_id \
             LEFT JOIN jobs parent ON parent.id=j.parent_job_id \
             WHERE p.key=?1 AND i.number=?2 AND e.seq=?3 \
               AND j.uri_segment=?4 AND a.uri_segment=?5 \
               AND (?6 IS NULL OR parent.uri_segment=?6) LIMIT 1",
                resolved_column = if table == "prompts" {
                    "answered_at"
                } else {
                    "responded_at"
                },
            );
            let mut rows = conn
                .query(
                    &sql,
                    params![
                        project,
                        number as i64,
                        exec_seq as i64,
                        target_node,
                        segment,
                        parent_node
                    ],
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(None);
            };
            let Some(surface) = row.opt_text(1)? else {
                return Ok(None);
            };
            Ok(Some(cairn_db::models::ResolutionReceipt {
                id: row.opt_text(0)?,
                surface,
                provider: row.opt_text(2)?,
                conversation: row.opt_text(3)?,
                actor: row.opt_text(4)?,
                resolved_at: row.i64(5)?,
            }))
        })
    })
    .await
}

impl Wake {
    pub fn as_str(self) -> &'static str {
        match self {
            Wake::Passive => "passive",
            Wake::Wake => "wake",
            Wake::Interrupt => "interrupt",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "passive" => Some(Wake::Passive),
            "wake" => Some(Wake::Wake),
            "interrupt" => Some(Wake::Interrupt),
            _ => None,
        }
    }

    /// Whether this wake level wakes an *idle* agent — so a push creator should
    /// nudge the recipient. `Passive` rides along on the next run and never
    /// wakes; `Wake` and `Interrupt` do. A muted source downgraded to `Passive`
    /// (see [`push_with_fingerprint`]) reports `false` here, which is how a
    /// creator skips nudging a muted recipient.
    pub fn wakes_idle(self) -> bool {
        matches!(self, Wake::Wake | Wake::Interrupt)
    }
}

/// Boundary: where a push lands on a *busy* agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Boundary {
    /// Next tool-call return.
    Event,
    /// Turn end.
    Turn,
}

impl Boundary {
    pub fn as_str(self) -> &'static str {
        match self {
            Boundary::Event => "event",
            Boundary::Turn => "turn",
        }
    }

    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "event" => Some(Boundary::Event),
            "turn" => Some(Boundary::Turn),
            _ => None,
        }
    }
}

/// One queued push.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Push {
    pub id: String,
    pub recipient: String,
    pub content_ref: String,
    pub wake: Wake,
    pub boundary: Boundary,
    pub key: String,
    pub created_at: i64,
    /// `None` = undelivered. When set, the durable event that sealed delivery.
    pub delivered_event_id: Option<String>,
}

pub(crate) fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

fn push_from_row(row: &cairn_db::turso::Row) -> DbResult<Push> {
    push_from_row_offset(row, 0)
}

/// [`push_from_row`] for a query that selects the push's eight columns starting
/// at `base` rather than at zero — the retirement backfill selects `rowid`
/// first, because its keyset walk needs it.
pub(crate) fn push_from_row_offset(row: &cairn_db::turso::Row, base: usize) -> DbResult<Push> {
    let wake = row.text(base + 3)?;
    let boundary = row.text(base + 4)?;
    Ok(Push {
        id: row.text(base)?,
        recipient: row.text(base + 1)?,
        content_ref: row.text(base + 2)?,
        wake: Wake::from_db(&wake)
            .ok_or_else(|| DbError::Row(format!("invalid push wake: {wake}")))?,
        boundary: Boundary::from_db(&boundary)
            .ok_or_else(|| DbError::Row(format!("invalid push boundary: {boundary}")))?,
        key: row.text(base + 5)?,
        created_at: row.i64(base + 6)?,
        delivered_event_id: row.opt_text(base + 7)?,
    })
}

/// Human-facing reminder line for a drained push. The CLI wraps it in a
/// `<system-reminder>` block at the transport edge. Until creators carry inline
/// content (slices B–D) the line references the push's `content_ref` and wake
/// level; that is enough for the agent to follow the ref to the live referent.
pub fn render_push(push: &Push) -> String {
    format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    )
}

/// Render drained pushes as the `attention:briefing` event payload the frontend
/// wake-card formatter consumes (CAIRN-1891): `{active, catchup}` arrays of
/// `{kind, headline, uri}` items, the same shape the legacy attention briefing
/// emits, so a delivered wake renders through the one wake-card path instead of a
/// raw text line. Rousing (`wake`/`interrupt`) pushes are `active`; passive
/// ride-along pushes are `catchup`. `uri` is the push's `content_ref`, which the
/// card's resource link opens for the full resolved content. The agent's prompt
/// still receives the resolved markdown separately ([`render_pushes_resolved`] in
/// `attention_delivery`); this is the UI record.
pub fn push_kind_headline(prefix: &str) -> (&str, &str) {
    match prefix {
        "review" => ("review", "Work product ready for review"),
        "question" => ("question", "Question awaiting an answer"),
        "permission" => ("permission", "Permission awaiting a decision"),
        "catchup" => ("catch-up", "New chat to catch up on"),
        "direct" => ("message", "Direct message"),
        "schedule" => ("schedule", "Scheduled wake"),
        "resolved" => ("resolved", "Issue resolved"),
        "tasks" => ("tasks", "Tasks need attention"),
        "turn-checks" => ("checks", "Turn-end check results"),
        "build-change" => ("system", "Cairn was rebuilt"),
        "post" => ("post", "New post"),
        "post-comment" => ("post", "New comment on your post"),
        "post-mention" => ("post", "A post referenced you"),
        other => (other, "Attention update"),
    }
}

pub fn pushes_to_briefing_json(pushes: &[Push]) -> String {
    pushes_to_briefing_json_with_resolutions(pushes, &std::collections::HashMap::new())
}

/// Structured wake projection for completed asks. Pending pushes continue to
/// use the receipt-free shape; callers that have observed completion attach the
/// canonical persisted receipt by referent URI.
pub fn pushes_to_briefing_json_with_resolutions(
    pushes: &[Push],
    resolutions: &std::collections::HashMap<String, cairn_db::models::ResolutionReceipt>,
) -> String {
    let mut active = Vec::new();
    let mut catchup = Vec::new();
    for push in pushes {
        let prefix = push
            .key
            .split_once(':')
            .map(|(p, _)| p)
            .unwrap_or(&push.key);
        let (kind, headline) = push_kind_headline(prefix);
        let mut item = serde_json::json!({
            "kind": kind,
            "headline": headline,
            "uri": push.content_ref,
        });
        let ask_ref = push.key.split_once(':').map(|(_, value)| value);
        if let Some(resolution) = ask_ref.and_then(|reference| resolutions.get(reference)) {
            item["resolution"] = serde_json::to_value(resolution).unwrap_or_default();
        }
        if push.wake == Wake::Passive {
            catchup.push(item);
        } else {
            active.push(item);
        }
    }
    serde_json::json!({ "active": active, "catchup": catchup }).to_string()
}

/// Like [`pushes_to_briefing_json`] but also carries the `resolved` markdown the
/// agent received, so the transcript detail modal can show the full content
/// rather than only the resource refs (CAIRN-1891). The frontend wake card reads
/// `active`/`catchup` and ignores `resolved`; the detail modal reads `resolved`.
pub fn push_event_content_json(pushes: &[Push], resolved: &str) -> String {
    let mut value: serde_json::Value = serde_json::from_str(&pushes_to_briefing_json(pushes))
        .unwrap_or_else(|_| serde_json::json!({ "active": [], "catchup": [] }));
    if let Some(obj) = value.as_object_mut() {
        obj.insert(
            "resolved".to_string(),
            serde_json::Value::String(resolved.to_string()),
        );
    }
    value.to_string()
}

/// Join the rendered text of several pushes into one block, or `None` when the
/// slice is empty (so callers can fold it into an optional prompt section).
pub fn render_pushes(pushes: &[Push]) -> Option<String> {
    if pushes.is_empty() {
        return None;
    }
    Some(
        pushes
            .iter()
            .map(render_push)
            .collect::<Vec<_>>()
            .join("\n\n"),
    )
}

/// Insert a push, superseding any existing **undelivered** push with the same
/// `(recipient, key)` in place. Returns the id of the resulting undelivered row
/// (on supersession this is the pre-existing row's id, not the discarded
/// freshly-generated one) and the **effective** wake the row was created with —
/// which may be `Passive` even though `Wake` was requested, when the recipient
/// has muted the push's source (see [`push_with_fingerprint`]). Callers key
/// their nudge decision off the effective wake via [`Wake::wakes_idle`].
pub async fn has_push_identity(db: &LocalDb, recipient: &str, key: &str) -> DbResult<bool> {
    let recipient = recipient.to_string();
    let key = cairn_common::uri::canonicalize_uri_identity(key);
    db.read(|conn| {
        let recipient = recipient.clone();
        let key = key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM attention_pushes WHERE recipient=?1 AND key=?2 LIMIT 1",
                    params![recipient.as_str(), key.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
}

pub async fn push(
    db: &LocalDb,
    recipient: &str,
    content_ref: &str,
    wake: Wake,
    boundary: Boundary,
    key: &str,
) -> DbResult<(String, Wake)> {
    push_with_fingerprint(db, recipient, content_ref, wake, boundary, key, None).await
}

/// Downgrade an issue- or checks-sourced push to `Passive` when the recipient
/// holds an active **mute** on that source.
/// This is the creation-time sibling of [`lazy_resolve_live`]'s drain-time issue
/// resolution: applying it centrally in [`push_with_fingerprint`] makes the
/// muted-source bug-class structural — no push creator on a recognized source
/// can forget to
/// consult mute. The subject issue URI is the push key's suffix
/// (`{prefix}:{issue_uri}`), which is exactly the `source_ref` an issue
/// subscription stores, so no DB lookup of the issue is needed. Non-`Wake`
/// levels (`Passive` already lowest, `Interrupt` never downgraded) and non-issue
/// prefixes (`catchup` / `direct`) short-circuit without a query. A
/// `turn-checks` suffix is already the canonical checks URI stored by a
/// `kind:"checks"` mute.
/// A direct's source is its sender (peer/user axis), not the subject URI, so the
/// direct creator applies the same [`crate::orchestrator::wakes::mute_downgrade`]
/// rule explicitly at its own site rather than here.
async fn source_mute_downgrade(
    db: &LocalDb,
    recipient: &str,
    key: &str,
    requested: Wake,
) -> DbResult<Wake> {
    if requested != Wake::Wake {
        return Ok(requested);
    }
    let Some((prefix, source_uri)) = key.split_once(':') else {
        return Ok(requested);
    };
    let (source_kind, fact_kind) = match prefix {
        "review" | "question" | "permission" | "resolved" => ("issue", prefix),
        "turn-checks" => ("condition", "checks_settled"),
        // A post push's suffix is the post's URI, which a ref-less Posts
        // subscription matches as a standing watch, so a muted Posts watcher's
        // wake is downgraded here into the same ride-along every other muted
        // source produces.
        _ => match crate::orchestrator::wakes::post_push_source(prefix) {
            Some(source) => source,
            None => return Ok(requested),
        },
    };
    crate::orchestrator::wakes::mute_downgrade(
        db,
        recipient,
        source_kind,
        Some(source_uri),
        fact_kind,
        requested,
    )
    .await
    .map_err(DbError::Row)
}

/// Like [`push`], but stamps a `fingerprint` — a lightweight content key of the
/// underlying reviewable state — on the row. Only the review creator
/// (`lifecycle::create_review_push_on_turn_end`) uses this: it compares the
/// latest review push's fingerprint ([`latest_push_fingerprint`]) against the
/// current reviewable state and skips re-creating an unchanged review push
/// (CAIRN-1889, change-triggered review). All other push kinds are
/// event-triggered and leave the fingerprint NULL.
pub async fn push_with_fingerprint(
    db: &LocalDb,
    recipient: &str,
    content_ref: &str,
    wake: Wake,
    boundary: Boundary,
    key: &str,
    fingerprint: Option<&str>,
) -> DbResult<(String, Wake)> {
    // Consult mute centrally for recognized source prefixes; a muted source's `Wake`
    // becomes `Passive` so the row is created as a ride-along rather than a
    // rousing wake (CAIRN-1900). The effective wake is returned so the caller
    // skips nudging a downgraded recipient.
    let wake = source_mute_downgrade(db, recipient, key, wake).await?;
    let recipient = recipient.to_string();
    let content_ref = cairn_common::uri::canonicalize_uri_identity(content_ref);
    let key = cairn_common::uri::canonicalize_uri_identity(key);
    let fingerprint = fingerprint.map(|s| s.to_string());
    let now = now_ts();
    let id = db
        .write(|conn| {
            let recipient = recipient.clone();
            let content_ref = content_ref.clone();
            let key = key.clone();
            let fingerprint = fingerprint.clone();
            let id = Uuid::new_v4().to_string();
            Box::pin(async move {
                // Supersede in place: update the existing undelivered same-key row if
                // one exists, otherwise insert a fresh row. db.write serializes the
                // transaction so the update-then-insert is atomic; the partial unique
                // index on (recipient, key) WHERE delivered_event_id IS NULL guards
                // against a concurrent double-insert. (An ON CONFLICT upsert keyed on
                // that partial index isn't accepted by the Turso SQL parser.)
                let updated = conn
                    .execute(
                        "UPDATE attention_pushes
                     SET content_ref=?1, wake=?2, boundary=?3, created_at=?4, fingerprint=?5
                     WHERE recipient=?6 AND key=?7
                       AND delivered_event_id IS NULL AND retired_at IS NULL",
                        params![
                            content_ref.as_str(),
                            wake.as_str(),
                            boundary.as_str(),
                            now,
                            fingerprint.as_deref(),
                            recipient.as_str(),
                            key.as_str()
                        ],
                    )
                    .await?;
                if updated == 0 {
                    conn.execute(
                        "INSERT INTO attention_pushes
                       (id, recipient, content_ref, wake, boundary, key, created_at, fingerprint)
                     VALUES (?1,?2,?3,?4,?5,?6,?7,?8)",
                        params![
                            id.as_str(),
                            recipient.as_str(),
                            content_ref.as_str(),
                            wake.as_str(),
                            boundary.as_str(),
                            key.as_str(),
                            now,
                            fingerprint.as_deref()
                        ],
                    )
                    .await?;
                }
                // The updated row keeps its original id; read back the canonical
                // undelivered row's id (the partial unique index guarantees one).
                let mut rows = conn
                    .query(
                        "SELECT id FROM attention_pushes
                     WHERE recipient=?1 AND key=?2
                       AND delivered_event_id IS NULL AND retired_at IS NULL
                     LIMIT 1",
                        params![recipient.as_str(), key.as_str()],
                    )
                    .await?;
                let row = rows
                    .next()
                    .await?
                    .ok_or_else(|| DbError::Row("push row missing after upsert".into()))?;
                row.text(0)
            })
        })
        .await?;
    Ok((id, wake))
}

/// The `fingerprint` of the most recent push (delivered OR undelivered) for
/// `(recipient, key)`, newest by insertion order. `created_at` is only second
/// precision, so SQLite's monotonic rowid is the deterministic tie-breaker rather
/// than the random UUID primary key. Outer `None` = no such push exists;
/// `Some(None)` = a push with a NULL fingerprint. Content-state creators
/// use this to skip re-firing unchanged review and terminal-resolution pushes,
/// including after the prior row has already been delivered.
pub async fn latest_push_fingerprint(
    db: &LocalDb,
    recipient: &str,
    key: &str,
) -> DbResult<Option<Option<String>>> {
    let recipient = recipient.to_string();
    let key = key.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        let key = key.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT fingerprint FROM attention_pushes
                     WHERE recipient=?1 AND key=?2
                     ORDER BY created_at DESC, rowid DESC
                     LIMIT 1",
                    params![recipient.as_str(), key.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.opt_text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// All undelivered pushes for a recipient, oldest first. The general drain
/// primitive; wake/boundary filtering for specific drain sites is a caller
/// concern (see [`pending_at_boundary`] for the per-boundary view).
pub async fn list_pending(db: &LocalDb, recipient: &str) -> DbResult<Vec<Push>> {
    let recipient = recipient.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id
                     FROM attention_pushes
                     WHERE recipient=?1 AND delivered_event_id IS NULL AND retired_at IS NULL
                     ORDER BY created_at ASC, id ASC",
                    params![recipient.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(push_from_row(&row)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Undelivered pushes for a recipient at a given boundary, oldest first —
/// **every** wake level, including `passive`. A thin filtered view over
/// [`list_pending`] scoped to one boundary: wake level governs whether an *idle*
/// agent is roused, not whether a push lands at this boundary on an active one.
pub async fn pending_at_boundary(
    db: &LocalDb,
    recipient: &str,
    boundary: Boundary,
) -> DbResult<Vec<Push>> {
    let recipient = recipient.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id
                     FROM attention_pushes
                     WHERE recipient=?1 AND delivered_event_id IS NULL AND retired_at IS NULL
                       AND boundary=?2
                     ORDER BY created_at ASC, id ASC",
                    params![recipient.as_str(), boundary.as_str()],
                )
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push(push_from_row(&row)?);
            }
            Ok(out)
        })
    })
    .await
}

/// Undelivered pushes for a recipient that are still **live** (their referent
/// has not resolved), oldest first. The resume-edge drain: both rousing and
/// `passive` ride-along pushes are returned, with [`lazy_resolve_live`] filtering
/// out any whose referent already resolved before the drain.
pub async fn list_pending_live(db: &LocalDb, recipient: &str) -> DbResult<Vec<Push>> {
    retain_live(db, list_pending(db, recipient).await?).await
}

/// Drop the pushes whose referent already resolved, preserving input order, and
/// retire the ones proved terminal. One batched [`resolve_verdicts`] for the
/// whole set (see its note on why a drain must not resolve row-at-a-time).
async fn retain_live(db: &LocalDb, pushes: Vec<Push>) -> DbResult<Vec<Push>> {
    let verdicts = resolve_verdicts(db, &pushes).await?;
    // Write down what this drain just proved. Retiring here is what makes the
    // queue converge: the same resolution that filters a push out of THIS drain
    // stops it being resolved again in every future one. It costs a write only
    // while there is something to retire, so a settled recipient pays nothing.
    retire_terminal(db, &pushes, &verdicts).await?;
    Ok(pushes
        .into_iter()
        .zip(verdicts)
        .filter_map(|(push, verdict)| verdict.is_live().then_some(push))
        .collect())
}

/// Retire the pushes a drain just proved terminal, returning how many rows this
/// call actually changed. Zero means there was nothing to retire, or another
/// writer got there first — both are ordinary.
///
/// Retirement never sets `delivered_event_id`, never deletes, and never advances
/// a read cursor. It marks a row invisible to delivery and leaves it legible to
/// audit.
///
/// **Losing is correct.** Classification happens outside the write transaction,
/// so between deciding and writing, the row may have been delivered or
/// superseded in place by a fresh push carrying new content. Each update is
/// therefore guarded on the row still being pending AND still being the row that
/// was classified: `content_ref` is the resolver's actual input, and `created_at`
/// is rewritten by every supersession. If either moved, this update matches
/// nothing, the row stays pending, and the next drain reclassifies whatever is
/// there now. A delivery or a supersession always wins over a retirement.
///
/// (`created_at` has second precision, so a supersession landing in the same
/// second with an identical `content_ref` can slip past the guard. That case
/// retires a row naming the same referent with the same content as the one that
/// was classified terminal, which is the conclusion the resolver would reach
/// again anyway; a genuinely changed source state changes the fingerprint and
/// inserts a fresh row rather than superseding.)
pub async fn retire_terminal(
    db: &LocalDb,
    pushes: &[Push],
    verdicts: &[Verdict],
) -> DbResult<usize> {
    let doomed: Vec<(String, String, i64, &'static str)> = pushes
        .iter()
        .zip(verdicts)
        .filter_map(|(push, verdict)| {
            verdict.retirement_reason().map(|reason| {
                (
                    push.id.clone(),
                    push.content_ref.clone(),
                    push.created_at,
                    reason.as_str(),
                )
            })
        })
        .collect();
    if doomed.is_empty() {
        return Ok(0);
    }
    let now = now_ts();
    db.write(move |conn| {
        let doomed = doomed.clone();
        Box::pin(async move {
            let mut retired = 0usize;
            for (id, content_ref, created_at, reason) in &doomed {
                let affected = conn
                    .execute(
                        "UPDATE attention_pushes
                            SET retired_at=?1, retirement_reason=?2
                          WHERE id=?3
                            AND delivered_event_id IS NULL AND retired_at IS NULL
                            AND content_ref=?4 AND created_at=?5",
                        params![now, *reason, id.as_str(), content_ref.as_str(), *created_at],
                    )
                    .await?;
                retired += affected as usize;
            }
            Ok(retired)
        })
    })
    .await
}

/// Undelivered pushes for a recipient at a given boundary that are still
/// **live** — **every** wake level, including `passive`. The busy-agent boundary
/// drain: a thin [`lazy_resolve_live`] filter over [`pending_at_boundary`]. Wake
/// level governs idle-waking, not whether an active agent sees a push, so a
/// passive push rides along at its boundary on an agent that is already running.
pub async fn pending_deliverable_live(
    db: &LocalDb,
    recipient: &str,
    boundary: Boundary,
) -> DbResult<Vec<Push>> {
    retain_live(db, pending_at_boundary(db, recipient, boundary).await?).await
}

/// Whether the recipient has any undelivered *rousing* (`wake`/`interrupt`) push
/// that is still live, regardless of boundary. The idle-flush resume gate's
/// predicate: a rousing push is a reason to wake an idle agent. `passive` pushes
/// are excluded by construction, so they never wake — they only ride along on a
/// resume that happens for some other reason (drained by [`list_pending_live`]).
pub async fn has_pending_waking_live(db: &LocalDb, recipient: &str) -> DbResult<bool> {
    // A push queued before a thread was closed stays pending and auditable, but
    // it is not a reason to resume the session: closure is what suspends
    // delivery, and reopening is what restores it. Filtering at the resume gate
    // rather than deleting the rows is what makes the transition reversible.
    if crate::threads::is_dormant_thread_session(db, recipient).await {
        return Ok(false);
    }
    let rousing: Vec<Push> = list_pending(db, recipient)
        .await?
        .into_iter()
        .filter(|push| push.wake != Wake::Passive)
        .collect();
    let verdicts = resolve_verdicts(db, &rousing).await?;
    retire_terminal(db, &rousing, &verdicts).await?;
    Ok(verdicts.into_iter().any(Verdict::is_live))
}

/// The keys of a recipient's undelivered *rousing* (`wake`/`interrupt`) pushes,
/// ignoring liveness. Diagnostic only — [`has_pending_waking_live`] is the resume
/// gate; this names the pushes that were present so a decline (they all
/// lazy-resolve dead, or the recipient is self-suspended) can be logged instead
/// of failing silently (CAIRN-2410, the lost coordinator review wake).
pub async fn pending_waking_keys(db: &LocalDb, recipient: &str) -> DbResult<Vec<String>> {
    let pending = list_pending(db, recipient).await?;
    Ok(pending
        .into_iter()
        .filter(|push| push.wake != Wake::Passive)
        .map(|push| push.key)
        .collect())
}

/// Delete an undelivered push by id. Dismissal only applies while the push is
/// still pending; a concurrent delivery that stamps `delivered_event_id` first
/// makes this a no-op.
pub async fn delete_pending_by_id(db: &LocalDb, id: &str) -> DbResult<()> {
    let id = id.to_string();
    db.write(|conn| {
        let id = id.clone();
        Box::pin(async move {
            conn.execute(
                "DELETE FROM attention_pushes
                 WHERE id = ?1 AND delivered_event_id IS NULL AND retired_at IS NULL",
                params![id.as_str()],
            )
            .await?;
            Ok(())
        })
    })
    .await
}

/// Stamp each push as delivered by `event_id`, first-writer-wins: the
/// `delivered_event_id IS NULL` guard makes a duplicate stamp a no-op. Returns
/// the number of rows newly stamped. This is the standalone form; delivery sites
/// call [`stamp_delivered_conn`] inside the carrying event's own transaction so
/// event and stamp commit together.
pub async fn stamp_delivered(db: &LocalDb, push_ids: &[String], event_id: &str) -> DbResult<usize> {
    if push_ids.is_empty() {
        return Ok(0);
    }
    let push_ids: Vec<String> = push_ids.to_vec();
    let event_id = event_id.to_string();
    db.write(|conn| {
        let push_ids = push_ids.clone();
        let event_id = event_id.clone();
        Box::pin(async move { stamp_delivered_conn(conn, &push_ids, &event_id).await })
    })
    .await
}

/// Stamp pushes delivered **inside an existing transaction** — the load-bearing
/// atomic delivery seam (`docs/attention-redesign.md` Delivery section). Callers
/// run this in the same `db.write` as the carrying event's `INSERT`, so if the
/// transaction rolls back both the event and the stamp are lost together and the
/// push redelivers. First-writer-wins under the `delivered_event_id IS NULL`
/// guard makes a duplicate stamp a no-op. Returns the number of rows newly
/// stamped.
pub async fn stamp_delivered_conn(
    conn: &cairn_db::turso::Connection,
    push_ids: &[String],
    event_id: &str,
) -> DbResult<usize> {
    let mut stamped = 0usize;
    for id in push_ids {
        let affected = conn
            .execute(
                "UPDATE attention_pushes SET delivered_event_id=?1
                 WHERE id=?2 AND delivered_event_id IS NULL AND retired_at IS NULL",
                params![event_id, id.as_str()],
            )
            .await?;
        stamped += affected as usize;
    }
    Ok(stamped)
}

/// Undo a delivery whole — the carrying event and every stamp pointing at it —
/// after the turn that was to carry it never started.
///
/// The stamp commits atomically with the carrying event, which on a resume is
/// necessarily *before* the backend process exists: the pushes have to ride in
/// the prompt that spawn carries. So every launch has a window where the rows say
/// "delivered" and no agent has read them, and a launch can still fail inside it
/// — no credential, no CLI binary, a refused or dormant session. Left alone the
/// stamp seals a delivery that never happened: the push is spent, the node it was
/// minted for is never woken for that fact again, and nothing reports it, because
/// a spent push looks exactly like a read one. That is the silent never-wakes
/// failure (CAIRN-2410) arriving from the delivery end rather than the routing
/// end.
///
/// The event is removed along with the stamps, and that pairing is the point.
/// Delivery is defined as "carried by a durable event", so a stamp and its event
/// are one fact and must be undone as one: clearing only the stamp would leave a
/// briefing in the transcript for a turn no agent ever ran, which the next
/// successful resume then duplicates when it redelivers the same push. Undoing
/// both restores exactly the state a rolled-back delivery transaction would have
/// left, which is the guarantee the atomic seam already promises for a crash
/// between injection and commit.
///
/// Keyed on the carrying event rather than on push ids, so it can only ever
/// revert the delivery this launch itself wrote.
pub async fn revert_delivery(db: &LocalDb, carrying_event_id: &str) -> DbResult<usize> {
    let event_id = carrying_event_id.to_string();
    db.write(|conn| {
        let event_id = event_id.clone();
        Box::pin(async move {
            let restored = conn
                .execute(
                    "UPDATE attention_pushes SET delivered_event_id=NULL
                     WHERE delivered_event_id=?1",
                    params![event_id.as_str()],
                )
                .await? as usize;
            conn.execute("DELETE FROM events WHERE id=?1", params![event_id.as_str()])
                .await?;
            Ok(restored)
        })
    })
    .await
}

/// The parent's last-seen read position in the child chat `source` (the child
/// job id whose `{node}/chat` a catch-up push renders), or `None` if the parent
/// has never been shown catch-up for it. Catch-up resolves the start of its
/// delivered window against this single cursor (CAIRN-1894), so a second message
/// before delivery reuses the same start and the window still spans from the
/// first unseen message.
pub async fn read_cursor(db: &LocalDb, recipient: &str, source: &str) -> DbResult<Option<i64>> {
    let recipient = recipient.to_string();
    let source = source.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        let source = source.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT position FROM attention_read_cursors
                     WHERE recipient=?1 AND source=?2 LIMIT 1",
                    params![recipient.as_str(), source.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.i64(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// When the parent's read cursor for `source` last advanced — the moment its last
/// catch-up for that child job was delivered, stamped inside the same transaction
/// as the carrying event. The catch-up digest anchors "messages since you last
/// looked" on it (CAIRN-3342). `None` when the parent has never been shown
/// catch-up for that child.
pub async fn read_cursor_updated_at(
    db: &LocalDb,
    recipient: &str,
    source: &str,
) -> DbResult<Option<i64>> {
    let recipient = recipient.to_string();
    let source = source.to_string();
    db.read(|conn| {
        let recipient = recipient.clone();
        let source = source.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT updated_at FROM attention_read_cursors
                     WHERE recipient=?1 AND source=?2 LIMIT 1",
                    params![recipient.as_str(), source.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(Some(row.i64(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
}

/// Distinct chat turns currently recorded across one job's runs — the job-scoped
/// chat tail, matching exactly what `{node}/chat` renders (the node chat loads
/// events for that one job's runs). Runs on a caller-supplied connection so the
/// catch-up cursor advance can compute the delivery-time tail inside the stamp
/// transaction.
async fn count_job_chat_turns(conn: &cairn_db::turso::Connection, job_id: &str) -> DbResult<i64> {
    let mut rows = conn
        .query(
            "SELECT COUNT(DISTINCT e.turn_id) FROM events e
             JOIN runs r ON e.run_id = r.id
             WHERE r.job_id = ?1 AND e.turn_id IS NOT NULL",
            params![job_id],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(row.i64(0)?),
        None => Ok(0),
    }
}

/// Advance the catch-up read cursor for each delivered `catchup:` push, INSIDE
/// the carrying event's stamp transaction (CAIRN-1894). For each push id, read
/// its `(recipient, key)`; when the key is `catchup:{child-job-id}`, count that
/// job's delivery-time chat tail and upsert
/// `attention_read_cursors(recipient, source=child-job-id)` to
/// `MAX(existing, tail)` so the cursor is monotonic — a duplicate or out-of-order
/// redelivery never rewinds it. Non-`catchup:` pushes leave cursors untouched.
///
/// The cursor is keyed by the child JOB id (not the issue), so it counts exactly
/// the transcript `{node}/chat` renders — one job's runs, not the whole issue's
/// sibling jobs and sub-task runs. The delivery-time tail equals the end of what
/// `render_push_resolved` just showed: both read the same job chat at the same
/// synchronous resume, with no new turn able to interleave between the render and
/// this advance. Because
/// it runs in the same transaction as [`stamp_delivered_conn`], a rolled-back
/// carrying event rolls back the advance too, and catch-up redelivers against the
/// old cursor.
pub async fn advance_read_cursors_conn(
    conn: &cairn_db::turso::Connection,
    push_ids: &[String],
) -> DbResult<()> {
    let now = now_ts();
    for id in push_ids {
        let mut rows = conn
            .query(
                "SELECT recipient, key FROM attention_pushes WHERE id=?1 LIMIT 1",
                params![id.as_str()],
            )
            .await?;
        let Some(row) = rows.next().await? else {
            continue;
        };
        let recipient = row.text(0)?;
        let key = row.text(1)?;
        let Some(child_job_id) = key.strip_prefix("catchup:") else {
            continue;
        };
        // The key carries the child job id whose `{node}/chat` the push renders.
        // Count exactly that job's turns so the cursor tracks the same transcript
        // (== the end of what the render just showed).
        let tail = count_job_chat_turns(conn, child_job_id).await?;
        let updated = conn
            .execute(
                "UPDATE attention_read_cursors
                 SET position=MAX(position, ?3), updated_at=?4
                 WHERE recipient=?1 AND source=?2",
                params![recipient.as_str(), child_job_id, tail, now],
            )
            .await?;
        if updated == 0 {
            conn.execute(
                "INSERT INTO attention_read_cursors(recipient, source, position, updated_at)
                 VALUES(?1,?2,?3,?4)",
                params![recipient.as_str(), child_job_id, tail, now],
            )
            .await?;
        }
    }
    Ok(())
}

/// Whether `push` should still deliver: `true` if its referent is still live,
/// `false` if it already resolved (skip, no wake). The key prefix selects the
/// referent table; the subject issue is resolved from `content_ref` (issue- or
/// node-level URIs both work via the shared URI accessors). Informational
/// prefixes (`catchup`/`direct`/`resolved`/unknown) are always live.
///
/// Resolution is coarse and issue-scoped. `review:` is live while an open
/// unmerged `merge_requests` row OR an unconfirmed create-pr/plan artifact
/// exists for the issue (mirroring the creation predicate in
/// `lifecycle::create_review_push_on_turn_end` — a plan-review push has no PR
/// row, so a PR-only check would wrongly drop it). `question:` / `permission:`
/// are live while an unanswered `prompts` / pending `permission_requests` row
/// exists AND the subject issue is not terminal — a blocker on a
/// closed/merged/failed issue is dead (nothing cancels those rows on
/// terminalization, so the terminal check is what retires the push).
pub async fn lazy_resolve_live(db: &LocalDb, push: &Push) -> DbResult<bool> {
    Ok(resolve_live(db, std::slice::from_ref(push)).await?[0])
}

/// Which resolution tables decide whether a push is still worth delivering,
/// selected by the push key's prefix. Every other prefix (`catchup:`,
/// `direct:`, `resolved:`, ...) is informational: it names no referent, is
/// unconditionally live, and costs no query.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum Referent {
    Review,
    Question,
    Permission,
}

/// The issue a push's liveness is decided against, together with the referent
/// class deciding it. Resolution is coarse and issue-scoped, so two pushes with
/// the same subject always resolve identically — which makes the subject, not
/// the push, the unit of batching.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct Subject {
    referent: Referent,
    project_key: String,
    number: i32,
}

/// Classify a push's `(key, content_ref)` into the subject its liveness depends
/// on. Those two fields are the predicate's entire input, which is why the
/// batch entry point takes them rather than whole rows.
///
/// `None` means there is nothing to resolve — an informational prefix, an
/// unparseable `content_ref`, or a ref that names no issue. Each of those is
/// unconditionally live: the predicate fails OPEN, so a push is never silently
/// dropped for a reason the resolver could not understand.
fn subject_of(key: &str, content_ref: &str) -> Option<Subject> {
    let prefix = key.split_once(':').map(|(p, _)| p).unwrap_or(key);
    let referent = match prefix {
        "review" => Referent::Review,
        "question" => Referent::Question,
        "permission" => Referent::Permission,
        _ => return None,
    };
    let parsed = parse_uri(content_ref)?;
    let project_key = parsed.project().map(cairn_common::uri::canonical_project)?;
    let number = parsed.issue_number()?;
    Some(Subject {
        referent,
        project_key,
        number,
    })
}

/// `?, ?, ?` for an `IN (...)` list of `n` bound values.
fn placeholders(n: usize) -> String {
    (0..n).map(|_| "?").collect::<Vec<_>>().join(", ")
}

fn text_params(values: &[String]) -> Vec<Value> {
    values.iter().cloned().map(Value::Text).collect()
}

/// The statement mapping a batch of issue NUMBERS to their issue rows.
///
/// Split out, like every statement below, so the plan regression test can plan
/// exactly what production runs rather than a small stand-in.
///
/// The project key is deliberately absent from the SQL and matched in Rust
/// instead. This planner keeps no table statistics and picks among `issues`'
/// indexes by predicate shape alone: with a `p.key IN (...)` term present it
/// drives from the projects key index and reaches issues through
/// `idx_issues_project_id`, which loads EVERY issue of every named project.
/// Measured on a live 4.9k-issue workspace that flip appears once the number
/// list passes a few dozen entries -- so a small-batch plan assertion cannot
/// see it and a real backlog always hits it. With no key term there is nothing
/// else to drive from, and the numbers seek `idx_issues_number` (migration
/// 0195), which is what ties this statement to the backlog. Matching the key in
/// Rust also folds case through `canonical_project`, the way the channel router
/// already does, so a project key stored capitalized still resolves.
fn subject_issues_sql(numbers: usize) -> String {
    format!(
        "SELECT p.key, i.number, i.id, i.status
           FROM issues i JOIN projects p ON p.id = i.project_id
          WHERE i.number IN ({})",
        placeholders(numbers)
    )
}

/// A batch of issues' merge requests: whether each is open, and when it opened.
///
/// `opened_at` rides along because the review generation test needs the newest
/// merge request per issue. Reading it here is what lets that test be a
/// comparison in Rust instead of a correlated subquery in SQL -- see
/// [`review_artifact_facts_sql`] for why that distinction is load-bearing.
fn review_merge_requests_sql(issues: usize) -> String {
    format!(
        "SELECT issue_id, status, opened_at FROM merge_requests WHERE issue_id IN ({})",
        placeholders(issues)
    )
}

/// Every artifact fact the `review:` arms need for a batch of issues, in one
/// statement.
///
/// It carries NO `artifact_type` predicate, and that absence is the whole point.
/// `artifacts` has two candidate indexes and this planner has no statistics to
/// choose between them, so any mention of `artifact_type` gives it a reason to
/// reach for `idx_artifacts_type` -- either directly, or, once the probe also
/// carries a correlated scalar subquery, as a `MULTI-INDEX AND` that
/// materializes every artifact of that type and intersects it with the job's,
/// once per job. Cost then follows artifact HISTORY rather than the backlog.
///
/// Measured on a live 16k-artifact workspace with a 443-push review backlog:
/// the four `EXISTS` statements this replaced cost 8.2 seconds, of which the
/// generation arm alone -- the only one carrying a correlated subquery -- was
/// 8.2 of them. This shape costs 70ms. Every enabled channel provider ran that
/// sweep independently every five seconds, so no sweep finished before the next
/// began (CAIRN-4207).
///
/// With no type predicate there is nothing for `idx_artifacts_type` to serve and
/// the only path left is the intended one, `jobs(issue_id)` into
/// `artifacts(job_id)`. The arms then fall out of one pass over these rows,
/// which is also why they can no longer drift apart: they read the same facts.
fn review_artifact_facts_sql(issues: usize) -> String {
    format!(
        "SELECT j.issue_id, a.artifact_type, a.confirmed, a.created_at
           FROM jobs j JOIN artifacts a ON a.job_id = j.id
          WHERE j.issue_id IN ({})",
        placeholders(issues)
    )
}

/// Why a push was retired. Each value is a resolver outcome that PROVED the
/// referent had resolved -- never an absence of evidence, and never a property
/// of the recipient. The stored strings are constrained by 0196's CHECK, so this
/// enum and that vocabulary are one thing in two places.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetirementReason {
    ReviewResolved,
    QuestionResolved,
    PermissionResolved,
}

impl RetirementReason {
    pub fn as_str(self) -> &'static str {
        match self {
            RetirementReason::ReviewResolved => "review_resolved",
            RetirementReason::QuestionResolved => "question_resolved",
            RetirementReason::PermissionResolved => "permission_resolved",
        }
    }
}

/// What the resolver concluded about one push.
///
/// The distinction that matters is between the two NON-live answers, because
/// today's boolean predicate collapses them and that collapse is why the queue
/// grows without bound (CAIRN-4182):
///
/// - [`Verdict::Terminal`] means the referent resolved and cannot un-resolve:
///   the merge request merged, the question was answered, the permission was
///   decided. Retiring is safe because there is no future in which this push
///   becomes deliverable again.
/// - [`Verdict::Suspended`] means not deliverable *now*, with no proof of
///   terminality: an unanswered question on a closed issue, a review push whose
///   issue carries no merge request or plan artifact at all. Reopening the issue
///   restores deliverability, so retiring would destroy a live obligation.
///
/// Ambiguity resolves to [`Verdict::Live`], never to `Terminal`. A missing
/// issue, an unparseable ref, or an unrecognized prefix keeps the push
/// deliverable exactly as before -- the predicate fails OPEN, because a push
/// wrongly dropped produces no signal at all (CAIRN-2410).
///
/// [`Verdict::is_live`] reproduces the previous boolean predicate exactly, term
/// for term. Retirement is strictly additional information layered on top of
/// unchanged delivery behavior.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Verdict {
    Live,
    Suspended,
    Terminal(RetirementReason),
}

impl Verdict {
    /// Whether this push is still worth delivering. Identical to the boolean the
    /// predicate returned before retirement existed.
    pub fn is_live(self) -> bool {
        matches!(self, Verdict::Live)
    }

    /// The reason to record when retiring, or `None` when the push must be left
    /// pending.
    pub fn retirement_reason(self) -> Option<RetirementReason> {
        match self {
            Verdict::Terminal(reason) => Some(reason),
            Verdict::Live | Verdict::Suspended => None,
        }
    }
}

/// Whether each push's referent is still unresolved, for a whole batch at once.
/// Returns one flag per input push, in input order.
///
/// This is the canonical liveness predicate; [`lazy_resolve_live`] is the
/// single-push form over it, and every drain goes through it.
///
/// **Why batched.** A drain runs on every MCP tool call
/// (`dispatch::augment_with_queued_dms`), and a push whose referent resolved is
/// skipped but never retired — so a long-lived recipient's undelivered backlog
/// only grows, and the *dead* pushes are precisely the ones that fall past the
/// cheap first arm into the expensive ones. Resolving row-at-a-time cost one
/// read transaction per push — each a `BEGIN CONCURRENT` MVCC snapshot — and
/// re-ran every arm for every push against a query shape the planner could not
/// index. A coordinator holding 47 stale review pushes was paying roughly 358k
/// B-tree row visits per tool call (CAIRN-4181).
///
/// Batched, a drain is ONE read transaction and at most six index-driven
/// queries, whatever the size of the backlog. The arms below are the same arms,
/// combined the same way; only their shape changed, from one issue per
/// statement to a bound set per statement. That shape is load-bearing rather
/// than cosmetic: the planner drives the artifact arms from `jobs(issue_id)`
/// only in the set form (see the plan assertions in `storage::migrations`).
pub async fn resolve_live(db: &LocalDb, pushes: &[Push]) -> DbResult<Vec<bool>> {
    Ok(resolve_verdicts(db, pushes)
        .await?
        .into_iter()
        .map(Verdict::is_live)
        .collect())
}

/// [`resolve_live`] without collapsing the two non-live answers: one
/// [`Verdict`] per input push, in input order.
///
/// A drain calls this rather than [`resolve_live`] when it is in a position to
/// write the conclusion down. Deciding terminality costs nothing extra -- it is
/// read out of the same set queries that already decide liveness -- so the
/// choice is only about whether the caller can act on it.
pub async fn resolve_verdicts(db: &LocalDb, pushes: &[Push]) -> DbResult<Vec<Verdict>> {
    let refs: Vec<(&str, &str)> = pushes
        .iter()
        .map(|push| (push.key.as_str(), push.content_ref.as_str()))
        .collect();
    resolve_verdict_refs(db, &refs).await
}

/// [`resolve_live`] for `(key, content_ref)` pairs that are not `Push` rows in
/// hand — the channel router resolves its review backlog straight out of its
/// own snapshot query. Sharing this entry point is what keeps ONE liveness
/// predicate in the system: a second copy expressed as SQL somewhere else
/// drifts from this one silently, and the two disagreeing means a review is
/// either texted after it was merged or never texted at all.
pub async fn resolve_live_refs(db: &LocalDb, refs: &[(&str, &str)]) -> DbResult<Vec<bool>> {
    Ok(resolve_verdict_refs(db, refs)
        .await?
        .into_iter()
        .map(Verdict::is_live)
        .collect())
}

/// [`resolve_verdicts`] for `(key, content_ref)` pairs rather than `Push` rows.
/// The single place liveness AND terminality are decided, so the two can never
/// disagree about the same referent.
pub async fn resolve_verdict_refs(db: &LocalDb, refs: &[(&str, &str)]) -> DbResult<Vec<Verdict>> {
    let subjects: Vec<Option<Subject>> = refs
        .iter()
        .map(|(key, content_ref)| subject_of(key, content_ref))
        .collect();
    let distinct: Vec<Subject> = subjects
        .iter()
        .flatten()
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    if distinct.is_empty() {
        return Ok(vec![Verdict::Live; refs.len()]);
    }
    let resolved = resolve_subjects(db, &distinct).await?;
    Ok(subjects
        .iter()
        .map(|subject| match subject {
            // An informational prefix or an unparseable ref names no referent,
            // so there is nothing that could have resolved.
            None => Verdict::Live,
            // A subject the batch could not resolve fails open too, matching the
            // "issue not found -> don't drop" rule below. Never `Terminal`:
            // absence of evidence must not retire a row.
            Some(subject) => resolved.get(subject).copied().unwrap_or(Verdict::Live),
        })
        .collect())
}

/// Resolve a set of distinct subjects in one read transaction.
///
/// Each arm of the predicate documented on [`lazy_resolve_live`] is answered
/// for the whole batch at once, and an arm's statement is issued only when some
/// subject in the batch needs it. Every statement is named above, and every one
/// of them is an index seek keyed by something the BACKLOG supplies: the
/// distinct issue numbers the batch names, then the distinct issue ids those
/// resolve to. Nothing here may be keyed by a type, a status, or any other
/// column whose cardinality is a property of the workspace rather than of the
/// batch -- that is the difference between a bounded sweep and one whose cost
/// grows with history (CAIRN-4207).
async fn resolve_subjects(
    db: &LocalDb,
    subjects: &[Subject],
) -> DbResult<HashMap<Subject, Verdict>> {
    let project_keys: HashSet<String> = subjects
        .iter()
        .map(|subject| subject.project_key.clone())
        .collect();
    let numbers: Vec<i64> = subjects
        .iter()
        .map(|subject| subject.number as i64)
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    let subjects = subjects.to_vec();

    db.read(move |conn| {
        let subjects = subjects.clone();
        let project_keys = project_keys.clone();
        let numbers = numbers.clone();
        Box::pin(async move {
            // 1. Subjects -> issues. The numbers seek `idx_issues_number`; the
            //    project key is matched here and again per subject below, so a
            //    number that exists in several projects cannot cross-resolve.
            let sql = subject_issues_sql(numbers.len());
            let binds: Vec<Value> = numbers.iter().copied().map(Value::Integer).collect();
            let mut issue_id_of: HashMap<(String, i32), String> = HashMap::new();
            let mut terminal: HashSet<String> = HashSet::new();
            let mut rows = conn.query(&sql, binds).await?;
            while let Some(row) = rows.next().await? {
                let project_key = cairn_common::uri::canonical_project(row.text(0)?);
                // Another project happening to use one of these numbers. The
                // statement cannot exclude it without costing the index, so the
                // project qualification is applied here instead.
                if !project_keys.contains(&project_key) {
                    continue;
                }
                let number = row.i64(1)? as i32;
                let issue_id = row.text(2)?;
                // Mirrors `crate::models::IssueStatus::is_terminal`.
                if matches!(
                    row.opt_text(3)?.as_deref(),
                    Some("merged" | "closed" | "failed")
                ) {
                    terminal.insert(issue_id.clone());
                }
                issue_id_of.insert((project_key, number), issue_id);
            }

            // Only probe the issues a given referent class actually asks about.
            let ids_for = |referent: Referent| -> Vec<String> {
                subjects
                    .iter()
                    .filter(|subject| subject.referent == referent)
                    .filter_map(|subject| {
                        issue_id_of.get(&(subject.project_key.clone(), subject.number))
                    })
                    .cloned()
                    .collect::<HashSet<_>>()
                    .into_iter()
                    .collect()
            };

            // 2. `review:` arms, from two statements: the batch's merge
            //    requests, then every artifact its jobs carry. Order matters --
            //    the generation test reads the merge-request timestamps the
            //    first pass collects.
            let review_ids = ids_for(Referent::Review);
            let mut open_mr = HashSet::new();
            let mut any_mr = HashSet::new();
            let mut newest_mr_opened_at: HashMap<String, i64> = HashMap::new();
            let mut unconfirmed_plan = HashSet::new();
            let mut any_plan = HashSet::new();
            let mut create_pr = HashSet::new();
            let mut create_pr_awaiting_mr = HashSet::new();
            if !review_ids.is_empty() {
                let sql = review_merge_requests_sql(review_ids.len());
                let mut rows = conn.query(&sql, text_params(&review_ids)).await?;
                while let Some(row) = rows.next().await? {
                    let issue_id = row.text(0)?;
                    // `status NOT IN ('merged','closed')` in SQL: a NULL status
                    // satisfies neither side, so it is not an open PR.
                    if matches!(row.opt_text(1)?.as_deref(), Some(status)
                        if status != "merged" && status != "closed")
                    {
                        open_mr.insert(issue_id.clone());
                    }
                    // `MAX(opened_at)` per issue, skipping NULLs exactly as the
                    // aggregate does. An issue with no merge request keeps no
                    // entry at all, which the generation test below reads as the
                    // 0 its `COALESCE(..., 0)` used to supply.
                    if let Some(opened_at) = row.opt_i64(2)? {
                        newest_mr_opened_at
                            .entry(issue_id.clone())
                            .and_modify(|newest| *newest = (*newest).max(opened_at))
                            .or_insert(opened_at);
                    }
                    any_mr.insert(issue_id);
                }

                let sql = review_artifact_facts_sql(review_ids.len());
                let mut rows = conn.query(&sql, text_params(&review_ids)).await?;
                while let Some(row) = rows.next().await? {
                    let issue_id = row.text(0)?;
                    match row.text(1)?.as_str() {
                        "plan" => {
                            // Terminality needs positive evidence, so "has a
                            // plan artifact" is tracked separately from "has an
                            // UNCONFIRMED one". Without the distinction an issue
                            // that never had a plan is indistinguishable from
                            // one whose plan review completed, and only the
                            // second may retire.
                            if row.opt_i64(2)? == Some(0) {
                                unconfirmed_plan.insert(issue_id.clone());
                            }
                            any_plan.insert(issue_id);
                        }
                        "create-pr" => {
                            // Which review GENERATION the evidence belongs to.
                            //
                            // A `create-pr` artifact written AFTER the newest
                            // merge request opened is a review that has not got
                            // its PR row yet. Within one generation the order is
                            // always artifact-then-PR (the artifact auto-confirms
                            // on write and the PR opens milliseconds later), so a
                            // newer artifact can only mean a NEW generation.
                            //
                            // This is what keeps a rerun deliverable. An issue
                            // whose earlier run merged carries a merged
                            // `merge_requests` row forever, so "a merge request
                            // exists and none is open" is true during the next
                            // run's pre-open window too -- and retiring on that
                            // would permanently drop a review that was about to
                            // become live (CAIRN-2410's failure mode, made
                            // durable). Liveness is unchanged: this window still
                            // reads not-live, exactly as before. It is only
                            // barred from reading TERMINAL.
                            let newest_mr =
                                newest_mr_opened_at.get(&issue_id).copied().unwrap_or(0);
                            if row
                                .opt_i64(3)?
                                .is_some_and(|created_at| created_at > newest_mr)
                            {
                                create_pr_awaiting_mr.insert(issue_id.clone());
                            }
                            create_pr.insert(issue_id);
                        }
                        // Every other artifact type rides along in the same
                        // index-driven read and decides nothing. Naming the two
                        // that do in SQL is precisely what cost the index.
                        _ => {}
                    }
                }
            }

            // 3. `question:` and `permission:` arms. Each returns the referent
            //    rows themselves rather than a pre-filtered id set, so one
            //    statement yields both "is any still open" (liveness) and "did
            //    any ever exist" (the evidence terminality requires). An issue
            //    with no prompt row at all has no question to have been
            //    answered, and must not retire.
            let question_ids = ids_for(Referent::Question);
            let mut open_question = HashSet::new();
            let mut any_question = HashSet::new();
            if !question_ids.is_empty() {
                let sql = format!(
                    "SELECT r.issue_id, p.response FROM runs r JOIN prompts p ON p.run_id = r.id
                      WHERE r.issue_id IN ({})",
                    placeholders(question_ids.len())
                );
                let mut rows = conn.query(&sql, text_params(&question_ids)).await?;
                while let Some(row) = rows.next().await? {
                    let issue_id = row.text(0)?;
                    if row.opt_text(1)?.is_none() {
                        open_question.insert(issue_id.clone());
                    }
                    any_question.insert(issue_id);
                }
            }
            let permission_ids = ids_for(Referent::Permission);
            let mut open_permission = HashSet::new();
            let mut any_permission = HashSet::new();
            if !permission_ids.is_empty() {
                let sql = format!(
                    "SELECT r.issue_id, pr.status FROM runs r
                       JOIN permission_requests pr ON pr.run_id = r.id
                      WHERE r.issue_id IN ({})",
                    placeholders(permission_ids.len())
                );
                let mut rows = conn.query(&sql, text_params(&permission_ids)).await?;
                while let Some(row) = rows.next().await? {
                    let issue_id = row.text(0)?;
                    if row.opt_text(1)?.as_deref() == Some("pending") {
                        open_permission.insert(issue_id.clone());
                    }
                    any_permission.insert(issue_id);
                }
            }

            let mut out = HashMap::with_capacity(subjects.len());
            for subject in &subjects {
                let Some(issue_id) =
                    issue_id_of.get(&(subject.project_key.clone(), subject.number))
                else {
                    // Issue not found -> don't drop, and never retire: the
                    // resolver cannot tell a deleted issue from one it simply
                    // failed to read.
                    out.insert(subject.clone(), Verdict::Live);
                    continue;
                };
                // Each arm answers liveness first, exactly as before, then asks
                // whether the NON-live case is backed by evidence of resolution.
                // Every `Suspended` below is a case that reopening the referent
                // makes deliverable again, which is why none of them may retire.
                let verdict = match subject.referent {
                    Referent::Review => {
                        if open_mr.contains(issue_id)
                            || unconfirmed_plan.contains(issue_id)
                            || (create_pr.contains(issue_id) && !any_mr.contains(issue_id))
                        {
                            Verdict::Live
                        } else if create_pr_awaiting_mr.contains(issue_id) {
                            // A rerun has written its create-pr artifact and its
                            // PR row has not landed. The issue's older merged
                            // merge request is evidence about the PREVIOUS
                            // generation, not this push, so it proves nothing
                            // here. Not deliverable yet, not terminal either.
                            Verdict::Suspended
                        } else if any_mr.contains(issue_id) || any_plan.contains(issue_id) {
                            // A merge request exists and is not open (merged or
                            // closed), or a plan exists with nothing left
                            // unconfirmed, and no newer create-pr artifact is
                            // waiting on a PR row. The review this push names
                            // has been had.
                            Verdict::Terminal(RetirementReason::ReviewResolved)
                        } else {
                            // No merge request, no plan: nothing durable to
                            // review yet. The create-pr pre-open window lands
                            // here once its PR row appears, and a push for an
                            // issue that never produced either stays pending
                            // rather than being retired on an assumption.
                            Verdict::Suspended
                        }
                    }
                    Referent::Question => {
                        if !terminal.contains(issue_id) && open_question.contains(issue_id) {
                            Verdict::Live
                        } else if any_question.contains(issue_id)
                            && !open_question.contains(issue_id)
                        {
                            // Answered. Terminal even on a terminal issue: an
                            // answer is durable and reopening cannot unmake it.
                            Verdict::Terminal(RetirementReason::QuestionResolved)
                        } else {
                            // Still unanswered, suppressed only because the issue
                            // is closed/merged/failed -- reopening restores it.
                            Verdict::Suspended
                        }
                    }
                    Referent::Permission => {
                        if !terminal.contains(issue_id) && open_permission.contains(issue_id) {
                            Verdict::Live
                        } else if any_permission.contains(issue_id)
                            && !open_permission.contains(issue_id)
                        {
                            Verdict::Terminal(RetirementReason::PermissionResolved)
                        } else {
                            Verdict::Suspended
                        }
                    }
                };
                out.insert(subject.clone(), verdict);
            }
            Ok::<HashMap<Subject, Verdict>, DbError>(out)
        })
    })
    .await
}

/// Whether the issue currently has an open (not merged/closed) merge request.
/// Extracted from the `review` lazy-resolve predicate above so the turn-end
/// check runner can gate `when:review` checks on an actually-open PR (it uses
/// only the PR arm, not the unconfirmed-artifact arm the review push also treats
/// as reviewable).
pub async fn has_open_pr_for_issue(db: &LocalDb, issue_id: &str) -> DbResult<bool> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            exists(
                conn,
                "SELECT 1 FROM merge_requests
                 WHERE issue_id=?1 AND status NOT IN ('merged','closed') LIMIT 1",
                &issue_id,
            )
            .await
        })
    })
    .await
}

async fn exists(conn: &cairn_db::turso::Connection, sql: &str, issue_id: &str) -> DbResult<bool> {
    let mut rows = conn.query(sql, params![issue_id]).await?;
    Ok(rows.next().await?.is_some())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn completed_ask_briefing_keeps_canonical_receipt_provenance() {
        let push = sample_push(
            "question:cairn://p/proj/2/1/planner/questions/q-1",
            "cairn://p/proj/2",
        );
        let receipt = cairn_db::models::ResolutionReceipt {
            id: Some("receipt-1".into()),
            surface: "channel_reply".into(),
            provider: Some("telegram".into()),
            conversation: Some("telegram:chat-42".into()),
            actor: Some("telegram:chat-42:user-7".into()),
            resolved_at: 1_786_590_456,
        };
        let resolutions = std::collections::HashMap::from([(
            "cairn://p/proj/2/1/planner/questions/q-1".to_string(),
            receipt,
        )]);
        let briefing: serde_json::Value = serde_json::from_str(
            &pushes_to_briefing_json_with_resolutions(&[push], &resolutions),
        )
        .unwrap();
        let resolution = &briefing["active"][0]["resolution"];
        assert_eq!(resolution["provider"], "telegram");
        assert_eq!(resolution["conversation"], "telegram:chat-42");
        assert_eq!(resolution["actor"], "telegram:chat-42:user-7");
        assert_eq!(resolution["resolvedAt"], 1_786_590_456_i64);
    }
    use crate::storage::LocalDb;

    const ISSUE_URI: &str = "cairn://p/proj/2";

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-push.db").await
    }

    /// Seed a project, an issue (`issue-1` / `cairn://p/proj/2`), a watcher job
    /// (the recipient), a child job, and a run for that issue so the FK and the
    /// referent-table resolution queries have rows to work against.
    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec-1','default','issue-1','p','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','issue-1','running','sess',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','exec-1','planner','planner','running','sess2',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','child-job','issue-1',1,1);
            ",
        )
        .await
        .unwrap();
    }

    fn sample_push(key: &str, content_ref: &str) -> Push {
        Push {
            id: "placeholder".into(),
            recipient: "watcher".into(),
            content_ref: content_ref.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: key.into(),
            created_at: 1,
            delivered_event_id: None,
        }
    }

    /// The rendered plan for a statement, one node per line.
    async fn query_plan(db: &LocalDb, sql: &str, binds: Vec<Value>) -> String {
        let sql = format!("EXPLAIN QUERY PLAN {sql}");
        db.read(move |conn| {
            let sql = sql.clone();
            let binds = binds.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, binds).await?;
                let mut plan = String::new();
                while let Some(row) = rows.next().await? {
                    plan.push_str(&row.text(3)?);
                    plan.push('\n');
                }
                Ok(plan)
            })
        })
        .await
        .unwrap()
    }

    /// Subject resolution must cost the BACKLOG, never the workspace.
    ///
    /// The plan is the only seam where that difference is visible. The
    /// regression this pins returned the right rows, from the right number of
    /// statements, in one read transaction -- every cheaper assertion passed
    /// while a single sweep took 8.2 seconds and every enabled provider ran one
    /// every five seconds (CAIRN-4207). Only the plan said that resolving a
    /// 443-push backlog meant walking 16k artifacts.
    ///
    /// This planner keeps no table statistics and chooses among a table's
    /// indexes by predicate SHAPE alone, so both statements below regressed on a
    /// term that reads like a narrowing: `p.key IN (...)` on the issue
    /// statement, and `artifact_type = 'create-pr'` under a correlated subquery
    /// on the artifact statement. Each handed the planner a second candidate
    /// index keyed by something the WORKSPACE sizes, and it took it.
    ///
    /// Batch sizes here are deliberately larger than one. The issue statement's
    /// old plan flipped only once the number list passed a few dozen entries, so
    /// a single-value assertion watched the regression go by.
    #[tokio::test]
    async fn subject_resolution_statements_stay_on_the_backlog() {
        let db = migrated_db().await;
        const BATCH: usize = 64;

        let issues = query_plan(
            &db,
            &subject_issues_sql(BATCH),
            (0..BATCH as i64).map(Value::Integer).collect(),
        )
        .await;
        assert!(
            issues.contains("idx_issues_number"),
            "the batch's issue numbers must seek (migration 0195):\n{issues}"
        );
        assert!(
            !issues.contains("idx_issues_project_id"),
            "reaching issues through the project index loads every issue of \
             every project the backlog names, which is the workspace:\n{issues}"
        );

        let ids: Vec<Value> = (0..BATCH)
            .map(|n| Value::Text(format!("issue-{n}")))
            .collect();

        let facts = query_plan(&db, &review_artifact_facts_sql(BATCH), ids.clone()).await;
        assert!(
            facts.contains("SEARCH j USING INDEX idx_jobs_issue_id")
                && facts.contains("SEARCH a USING INDEX idx_artifacts_job_id"),
            "the artifact facts must be reached from the batch's issues, \
             jobs(issue_id) into artifacts(job_id):\n{facts}"
        );
        assert!(
            !facts.contains("idx_artifacts_type"),
            "an artifact index keyed by TYPE is keyed by a property of the \
             workspace: every plan or create-pr artifact ever written matches \
             it, and intersecting that set per job is the 8.2-second sweep \
             coming back. Nothing in this statement may mention \
             artifact_type:\n{facts}"
        );

        let merge_requests = query_plan(&db, &review_merge_requests_sql(BATCH), ids).await;
        assert!(
            merge_requests.contains("SEARCH merge_requests USING INDEX idx_mr_issue"),
            "the merge-request arm seeks by issue (migration 0195):\n{merge_requests}"
        );
    }

    /// A drain's database cost must be a constant, not a function of how much
    /// backlog the recipient carries. This is the regression the batched
    /// resolver exists to prevent: `dispatch::augment_with_queued_dms` drains on
    /// EVERY MCP tool call, and because a push whose referent resolved is
    /// skipped but never retired, a long-lived recipient's backlog only grows.
    /// Resolving row-at-a-time made the per-tool-call cost scale with a number
    /// nothing bounds (CAIRN-4181).
    ///
    /// Asserting read TRANSACTIONS rather than wall time makes this
    /// deterministic: each one is a `BEGIN CONCURRENT` MVCC snapshot, and the
    /// old drain opened one per push on top of the arms it then ran.
    #[tokio::test]
    async fn drain_transaction_count_does_not_grow_with_the_backlog() {
        let db = migrated_db().await;
        seed(&db).await;
        // Distinct subjects, not one subject repeated: batching that only
        // deduplicated would pass a weaker version of this test.
        for number in 100..140 {
            db.execute(
                "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
                 VALUES('issue-' || ?1, 'p', ?1, 'Extra', 'active', 'active', 'none', 1, 1)",
                params![number as i64],
            )
            .await
            .unwrap();
        }

        // One stale review push: the shape a healthy recipient carries.
        push(
            &db,
            "watcher",
            "cairn://p/proj/100",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/100",
        )
        .await
        .unwrap();
        let before = db.read_transaction_count();
        assert!(pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
        let small = db.read_transaction_count() - before;

        // Thirty-nine more, spanning all three resolvable referent classes, so
        // every arm is exercised at once.
        for number in 101..140 {
            let prefix = match number % 3 {
                0 => "review",
                1 => "question",
                _ => "permission",
            };
            push(
                &db,
                "watcher",
                &format!("cairn://p/proj/{number}"),
                Wake::Wake,
                Boundary::Event,
                &format!("{prefix}:cairn://p/proj/{number}"),
            )
            .await
            .unwrap();
        }
        let before = db.read_transaction_count();
        assert!(pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
        let large = db.read_transaction_count() - before;

        assert_eq!(
            small, large,
            "a 40x larger backlog must cost the same number of read transactions; \
             {small} for one push vs {large} for forty means resolution is per-push \
             again"
        );
        assert!(
            large <= 3,
            "a drain is the pending-row read plus one batched resolution, got {large}"
        );
    }

    /// The batch resolves each push against its OWN subject. A single set query
    /// per arm makes it possible to smear one issue's verdict across the batch,
    /// so this pins live and dead pushes together in one drain.
    #[tokio::test]
    async fn a_mixed_batch_resolves_each_push_against_its_own_subject() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('issue-live','p',7,'Live','active','active','none',1,1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('job-live','p','issue-live','running',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-live','job-live','p','issue-live','t','b','main','open',1,1);",
        )
        .await
        .unwrap();

        // `issue-1` (proj/2) has no PR, no artifact -> dead.
        // `issue-live` (proj/7) has an open PR -> live.
        // `direct:` has no referent -> always live.
        let batch = vec![
            sample_push("review:cairn://p/proj/2", ISSUE_URI),
            sample_push("review:cairn://p/proj/7", "cairn://p/proj/7"),
            sample_push("direct:whoever", "cairn://p/proj/2"),
            sample_push("review:cairn://p/proj/999", "cairn://p/proj/999"),
        ];
        assert_eq!(
            resolve_live(&db, &batch).await.unwrap(),
            // The last one names an issue that does not exist: fail OPEN, never
            // silently dropped.
            vec![false, true, true, true]
        );
    }

    /// Batching must not let one project's issue numbers resolve against
    /// another's. The set query binds numbers and project keys as independent
    /// lists, so their cross product reaches the row loop; only the exact
    /// (project, number) pairing may match.
    #[tokio::test]
    async fn subjects_do_not_cross_resolve_between_projects() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p2','w','Other','other','/tmp/other',1,1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
               VALUES('issue-other','p2',2,'Other','active','active','none',1,1);
             INSERT INTO jobs(id, project_id, issue_id, status, created_at, updated_at)
               VALUES('job-other','p2','issue-other','running',1,1);
             INSERT INTO merge_requests(id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
               VALUES('mr-other','job-other','p2','issue-other','t','b','main','open',1,1);",
        )
        .await
        .unwrap();

        // Both are issue number 2. Only `other/2` has the open PR.
        let batch = vec![
            sample_push("review:cairn://p/proj/2", ISSUE_URI),
            sample_push("review:cairn://p/other/2", "cairn://p/other/2"),
        ];
        assert_eq!(
            resolve_live(&db, &batch).await.unwrap(),
            vec![false, true],
            "proj/2 must not inherit other/2's open PR"
        );
    }

    #[tokio::test]
    async fn production_payload_resolves_question_receipt_from_push_key() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute("INSERT INTO prompts(id, run_id, job_id, questions, response, uri_segment, created_at, answered_at, resolution_id, resolution_surface, resolution_provider, resolution_conversation, resolution_actor) VALUES('prompt-1','run-1','child-job','[]','yes','q-1',10,1786590456000,'receipt-q','channel_reply','telegram','telegram:chat-42','telegram:user-7')", ()).await.unwrap();
        let push = sample_push(
            "question:cairn://p/proj/2/1/planner/questions/q-1",
            ISSUE_URI,
        );
        let payload: serde_json::Value = serde_json::from_str(
            &push_event_content_json_with_resolutions(&db, &[push], "answer")
                .await
                .unwrap(),
        )
        .unwrap();
        let receipt = &payload["active"][0]["resolution"];
        assert_eq!(receipt["provider"], "telegram");
        assert_eq!(receipt["conversation"], "telegram:chat-42");
        assert_eq!(receipt["actor"], "telegram:user-7");
        assert_eq!(receipt["resolvedAt"], 1_786_590_456_000_i64);
    }

    #[tokio::test]
    async fn production_payload_resolves_permission_receipt_from_push_key() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute("INSERT INTO permission_requests(id, run_id, job_id, tool_use_id, tool_name, tool_input, status, response, uri_segment, created_at, responded_at, resolution_id, resolution_surface, resolution_provider, resolution_conversation, resolution_actor) VALUES('permission-1','run-1','child-job','tool-1','Bash','{}','approved','once','perm-1',10,1786590456000,'receipt-p','channel_reply','discord','discord:guild/channel','discord:user-9')", ()).await.unwrap();
        let push = sample_push(
            "permission:cairn://p/proj/2/1/planner/permissions/perm-1",
            ISSUE_URI,
        );
        let payload: serde_json::Value = serde_json::from_str(
            &push_event_content_json_with_resolutions(&db, &[push], "approved")
                .await
                .unwrap(),
        )
        .unwrap();
        let receipt = &payload["active"][0]["resolution"];
        assert_eq!(receipt["surface"], "channel_reply");
        assert_eq!(receipt["provider"], "discord");
        assert_eq!(receipt["conversation"], "discord:guild/channel");
        assert_eq!(receipt["actor"], "discord:user-9");
        assert_eq!(receipt["resolvedAt"], 1_786_590_456_000_i64);
    }
    async fn delivered_event(db: &LocalDb, id: &str) -> Option<String> {
        let id = id.to_string();
        db.read(|conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT delivered_event_id FROM attention_pushes WHERE id=?1",
                        params![id.as_str()],
                    )
                    .await?;
                let out = match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                };
                Ok::<Option<String>, DbError>(out)
            })
        })
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn muted_turn_checks_push_is_created_passively() {
        let db = migrated_db().await;
        seed(&db).await;
        let checks_uri = "cairn://p/proj/2/1/builder/checks";
        crate::orchestrator::wakes::mute(
            &db,
            "watcher",
            "condition",
            Some(checks_uri),
            None,
            None,
            None,
            "agent",
        )
        .await
        .unwrap();

        let (_, effective_wake) = push_with_fingerprint(
            &db,
            "watcher",
            checks_uri,
            Wake::Wake,
            Boundary::Event,
            &format!("turn-checks:{checks_uri}"),
            Some("state"),
        )
        .await
        .unwrap();

        assert_eq!(effective_wake, Wake::Passive);
        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].wake, Wake::Passive);
    }

    #[tokio::test]
    async fn push_inserts_a_pending_row() {
        let db = migrated_db().await;
        seed(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/builder",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].recipient, "watcher");
        assert_eq!(pending[0].wake, Wake::Wake);
        assert_eq!(pending[0].boundary, Boundary::Event);
        assert_eq!(pending[0].key, "review:cairn://p/proj/2");
        assert!(pending[0].delivered_event_id.is_none());
    }

    #[tokio::test]
    async fn push_supersedes_undelivered_same_key_in_place() {
        let db = migrated_db().await;
        seed(&db).await;
        let (first, _) = push(
            &db,
            "watcher",
            "ref-1",
            Wake::Passive,
            Boundary::Turn,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        let (second, _) = push(
            &db,
            "watcher",
            "ref-2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        // Same undelivered row replaced in place, content/wake/boundary updated.
        assert_eq!(first, second);
        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].content_ref, "ref-2");
        assert_eq!(pending[0].wake, Wake::Wake);
        assert_eq!(pending[0].boundary, Boundary::Event);
    }

    #[tokio::test]
    async fn two_directs_to_one_recipient_do_not_collapse() {
        let db = migrated_db().await;
        seed(&db).await;
        // Each direct is keyed by its own message id, so supersede-by-key never
        // merges two unread directs (CAIRN-1900): each is its own undelivered row.
        let (id1, w1) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/builder",
            Wake::Wake,
            Boundary::Event,
            "direct:msg-1",
        )
        .await
        .unwrap();
        let (id2, w2) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/builder",
            Wake::Wake,
            Boundary::Event,
            "direct:msg-2",
        )
        .await
        .unwrap();
        assert_ne!(id1, id2, "distinct direct keys must not supersede");
        // The `direct:` prefix is not issue-sourced, so the central mute downgrade
        // is a no-op and both stay rousing.
        assert_eq!(w1, Wake::Wake);
        assert_eq!(w2, Wake::Wake);
        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 2, "both unread directs remain queued");
    }

    #[tokio::test]
    async fn delivered_push_is_not_superseded() {
        let db = migrated_db().await;
        seed(&db).await;
        let (first, _) = push(
            &db,
            "watcher",
            "ref-1",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        assert_eq!(
            stamp_delivered(&db, std::slice::from_ref(&first), "event-1")
                .await
                .unwrap(),
            1
        );

        // The delivered row has left the partial index, so the same key inserts a
        // fresh second row rather than superseding the delivered one.
        let (second, _) = push(
            &db,
            "watcher",
            "ref-2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        assert_ne!(first, second);

        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, second);
        assert_eq!(pending[0].content_ref, "ref-2");
    }

    #[tokio::test]
    async fn list_pending_excludes_delivered() {
        let db = migrated_db().await;
        seed(&db).await;
        let (a, _) = push(
            &db,
            "watcher",
            "ref-a",
            Wake::Wake,
            Boundary::Event,
            "review:a",
        )
        .await
        .unwrap();
        let (b, _) = push(
            &db,
            "watcher",
            "ref-b",
            Wake::Wake,
            Boundary::Event,
            "question:b",
        )
        .await
        .unwrap();
        assert_eq!(stamp_delivered(&db, &[a], "ev").await.unwrap(), 1);

        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, b);
    }

    #[tokio::test]
    async fn list_pending_orders_by_created_at() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "
            INSERT INTO attention_pushes(id, recipient, content_ref, wake, boundary, key, created_at)
              VALUES('late','watcher','r','wake','event','k1',200);
            INSERT INTO attention_pushes(id, recipient, content_ref, wake, boundary, key, created_at)
              VALUES('early','watcher','r','wake','event','k2',100);
            ",
        )
        .await
        .unwrap();

        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(
            pending.iter().map(|p| p.id.as_str()).collect::<Vec<_>>(),
            vec!["early", "late"]
        );
    }

    #[tokio::test]
    async fn stamp_delivered_is_idempotent_under_null_guard() {
        let db = migrated_db().await;
        seed(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "ref",
            Wake::Wake,
            Boundary::Event,
            "review:x",
        )
        .await
        .unwrap();

        assert_eq!(
            stamp_delivered(&db, std::slice::from_ref(&id), "ev-1")
                .await
                .unwrap(),
            1
        );
        // Second stamp is a no-op; the original event id stands.
        assert_eq!(
            stamp_delivered(&db, std::slice::from_ref(&id), "ev-2")
                .await
                .unwrap(),
            0
        );
        assert_eq!(delivered_event(&db, &id).await, Some("ev-1".to_string()));
        assert!(list_pending(&db, "watcher").await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn pending_at_boundary_keeps_passive_filters_boundary() {
        let db = migrated_db().await;
        seed(&db).await;
        push(
            &db,
            "watcher",
            "r1",
            Wake::Wake,
            Boundary::Event,
            "review:a",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            "r2",
            Wake::Passive,
            Boundary::Event,
            "catchup:b",
        )
        .await
        .unwrap();
        push(&db, "watcher", "r3", Wake::Wake, Boundary::Turn, "review:c")
            .await
            .unwrap();

        // Boundary still filters (the Turn push is excluded), but the wake axis no
        // longer does: both Event-boundary pushes come back, passive included.
        let at_event = pending_at_boundary(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        let mut keys: Vec<&str> = at_event.iter().map(|p| p.key.as_str()).collect();
        keys.sort_unstable();
        assert_eq!(keys, vec!["catchup:b", "review:a"]);
    }

    #[tokio::test]
    async fn push_with_fingerprint_persists_and_latest_reads_it_back() {
        let db = migrated_db().await;
        seed(&db).await;

        // No prior push for the key -> outer None.
        assert!(latest_push_fingerprint(&db, "watcher", "review:k")
            .await
            .unwrap()
            .is_none());

        push_with_fingerprint(
            &db,
            "watcher",
            "ref-1",
            Wake::Wake,
            Boundary::Event,
            "review:k",
            Some("fp-A"),
        )
        .await
        .unwrap();
        assert_eq!(
            latest_push_fingerprint(&db, "watcher", "review:k")
                .await
                .unwrap(),
            Some(Some("fp-A".to_string()))
        );

        // Supersede in place updates the fingerprint on the same undelivered row.
        push_with_fingerprint(
            &db,
            "watcher",
            "ref-2",
            Wake::Wake,
            Boundary::Event,
            "review:k",
            Some("fp-B"),
        )
        .await
        .unwrap();
        assert_eq!(
            latest_push_fingerprint(&db, "watcher", "review:k")
                .await
                .unwrap(),
            Some(Some("fp-B".to_string()))
        );
        assert_eq!(list_pending(&db, "watcher").await.unwrap().len(), 1);

        // A plain push leaves the fingerprint NULL -> Some(None).
        push(
            &db,
            "watcher",
            "r",
            Wake::Passive,
            Boundary::Event,
            "resolved:k2",
        )
        .await
        .unwrap();
        assert_eq!(
            latest_push_fingerprint(&db, "watcher", "resolved:k2")
                .await
                .unwrap(),
            Some(None)
        );
    }

    #[tokio::test]
    async fn latest_fingerprint_uses_insertion_order_for_same_second_delivered_states() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "
            INSERT INTO attention_pushes
              (id, recipient, content_ref, wake, boundary, key, created_at, delivered_event_id, fingerprint)
            VALUES
              ('z-old-random-order', 'watcher', 'ref', 'wake', 'event', 'turn-checks:k', 42, 'event-a', 'red-a'),
              ('m-middle-random-order', 'watcher', 'ref', 'passive', 'event', 'turn-checks:k', 42, 'event-green', 'green'),
              ('a-new-random-order', 'watcher', 'ref', 'wake', 'event', 'turn-checks:k', 42, 'event-b', 'red-b');
            ",
        )
        .await
        .unwrap();

        assert_eq!(
            latest_push_fingerprint(&db, "watcher", "turn-checks:k")
                .await
                .unwrap(),
            Some(Some("red-b".to_string()))
        );
    }

    #[tokio::test]
    async fn lazy_resolve_review_lives_with_open_pr_skips_when_merged() {
        let db = migrated_db().await;
        seed(&db).await;
        let p = sample_push("review:cairn://p/proj/2", "cairn://p/proj/2/1/builder");

        // No merge_request row -> nothing open -> resolved.
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr','child-job','p','issue-1','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script("UPDATE merge_requests SET status='merged' WHERE id='mr';")
            .await
            .unwrap();
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_question_lives_until_answered() {
        let db = migrated_db().await;
        seed(&db).await;
        let p = sample_push(
            "question:cairn://p/proj/2/1/planner/questions/q-1",
            ISSUE_URI,
        );

        // No prompt -> resolved.
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script(
            "INSERT INTO prompts(id, run_id, questions, response, created_at)
             VALUES('q','run-1','[]',NULL,1);",
        )
        .await
        .unwrap();
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script("UPDATE prompts SET response='answered' WHERE id='q';")
            .await
            .unwrap();
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_permission_lives_until_decided() {
        let db = migrated_db().await;
        seed(&db).await;
        let p = sample_push(
            "permission:cairn://p/proj/2/1/builder/permissions/perm-1",
            ISSUE_URI,
        );

        assert!(!lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script(
            "INSERT INTO permission_requests(id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
             VALUES('perm','run-1','tu','bash','{}','pending',1);",
        )
        .await
        .unwrap();
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script("UPDATE permission_requests SET status='allowed' WHERE id='perm';")
            .await
            .unwrap();
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_question_dead_when_issue_terminal() {
        let db = migrated_db().await;
        seed(&db).await;
        let p = sample_push(
            "question:cairn://p/proj/2/1/planner/questions/q-1",
            ISSUE_URI,
        );
        db.execute_script(
            "INSERT INTO prompts(id, run_id, questions, response, created_at)
             VALUES('q','run-1','[]',NULL,1);",
        )
        .await
        .unwrap();
        // Pending prompt + active issue -> live.
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        // The issue terminalizes with the prompt still pending: the push is dead
        // even though nothing resolved the prompts row.
        db.execute_script("UPDATE issues SET status='merged' WHERE id='issue-1';")
            .await
            .unwrap();
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_review_lives_with_unconfirmed_plan_artifact_no_pr() {
        let db = migrated_db().await;
        seed(&db).await;
        // A plan-review push: content_ref is a /plan node URI, no merge_request.
        let p = sample_push("review:cairn://p/proj/2", "cairn://p/proj/2/1/planner/plan");
        // No PR and no artifact yet -> dead.
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-plan','child-job','plan',1,'{}',1,'plan',0,1,1);",
        )
        .await
        .unwrap();
        // Unconfirmed plan artifact, still no PR -> live (the plan-review fix).
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script("UPDATE artifacts SET confirmed=1 WHERE id='a-plan';")
            .await
            .unwrap();
        // Confirmed + no PR -> dead.
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_review_lives_with_confirmed_create_pr_no_mr() {
        let db = migrated_db().await;
        seed(&db).await;
        // The incident (CAIRN-2410): a builder wrote a `create-pr` artifact that
        // AUTO-confirmed on write (CAIRN-1219), and the PR has not opened yet
        // (no merge_requests row). Before the fix this window read DEAD, silently
        // dropping the idle coordinator's review wake. It must read LIVE.
        let p = sample_push("review:cairn://p/proj/2", "cairn://p/proj/2/1/builder");
        // No artifact and no PR -> dead.
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());

        db.execute_script(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-pr','child-job','create-pr',1,'{}',1,'create-pr',1,1,1);",
        )
        .await
        .unwrap();
        // Confirmed create-pr artifact, no MR row yet -> LIVE (arm 3, the fix).
        assert!(lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_review_dead_when_mr_merged_despite_confirmed_create_pr() {
        let db = migrated_db().await;
        seed(&db).await;
        // Guards against over-widening arm 3: once a merge_requests row exists,
        // arm 1 is authoritative. A merged PR is a dead review even though the
        // confirmed create-pr artifact that opened it still exists.
        let p = sample_push("review:cairn://p/proj/2", "cairn://p/proj/2/1/builder");
        db.execute_script(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-pr','child-job','create-pr',1,'{}',1,'create-pr',1,1,1);",
        )
        .await
        .unwrap();
        // Pre-open window: confirmed create-pr, no MR row -> live via arm 3.
        assert!(lazy_resolve_live(&db, &p).await.unwrap());

        // The PR opened and then merged. Arm 1's no-MR-row guard hands control to
        // the row: a merged row makes the review dead, arm 3 no longer applies.
        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr','child-job','p','issue-1','t','b','main','merged',1,1);",
        )
        .await
        .unwrap();
        assert!(!lazy_resolve_live(&db, &p).await.unwrap());
    }

    #[tokio::test]
    async fn lazy_resolve_informational_prefixes_are_always_live() {
        let db = migrated_db().await;
        seed(&db).await;
        for key in [
            "catchup:cairn://p/proj/2/1/child",
            "direct:cairn://p/proj/2",
            "resolved:cairn://p/proj/2",
            "weird",
        ] {
            let p = sample_push(key, ISSUE_URI);
            assert!(
                lazy_resolve_live(&db, &p).await.unwrap(),
                "{key} should be informational/live"
            );
        }
    }

    /// Seed an open merge_request for the subject issue so a `review:` push's
    /// referent resolves as live.
    async fn open_mr(db: &LocalDb) {
        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-open','child-job','p','issue-1','t','b','main','open',1,1);",
        )
        .await
        .unwrap();
    }

    const REVIEW_KEY: &str = "review:cairn://p/proj/2";

    /// The recorded retirement reason for a push, or `None` while it is pending.
    /// 0196's CHECK makes reason and timestamp inseparable, so the reason alone
    /// witnesses both.
    async fn retirement_reason_of(db: &LocalDb, id: &str) -> Option<String> {
        let id = id.to_string();
        db.read(move |conn| {
            let id = id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT retirement_reason FROM attention_pushes WHERE id=?1",
                        (id,),
                    )
                    .await?;
                Ok(match rows.next().await? {
                    Some(row) => row.opt_text(0)?,
                    None => None,
                })
            })
        })
        .await
        .unwrap()
    }

    async fn count_where(db: &LocalDb, predicate: &str) -> i64 {
        let sql = format!("SELECT COUNT(*) FROM attention_pushes WHERE {predicate}");
        db.read(move |conn| {
            let sql = sql.clone();
            Box::pin(async move {
                let mut rows = conn.query(&sql, ()).await?;
                rows.next().await?.expect("count row").i64(0)
            })
        })
        .await
        .unwrap()
    }

    async fn only_pending_id(db: &LocalDb) -> String {
        list_pending(db, "watcher").await.unwrap()[0].id.clone()
    }

    /// The central claim: a drain that proves a referent resolved writes that
    /// down ONCE, the row leaves every pending view, and it is still there to
    /// read afterwards. Retiring is not deleting and not delivering.
    #[tokio::test]
    async fn a_resolved_review_retires_once_and_survives_as_audit_history() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
        )
        .await
        .unwrap();
        let id = only_pending_id(&db).await;

        // While the merge request is open the push is live and untouched.
        assert_eq!(list_pending_live(&db, "watcher").await.unwrap().len(), 1);
        assert_eq!(retirement_reason_of(&db, &id).await, None);

        db.execute_script("UPDATE merge_requests SET status='merged' WHERE id='mr-open';")
            .await
            .unwrap();

        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert_eq!(
            retirement_reason_of(&db, &id).await.as_deref(),
            Some("review_resolved"),
            "a merged merge request proves the review this push named has been had"
        );

        // Retained as audit history, never delivered, invisible to every
        // pending view including the idle-resume gate.
        assert_eq!(count_where(&db, "1=1").await, 1, "retiring must not delete");
        assert_eq!(count_where(&db, "delivered_event_id IS NOT NULL").await, 0);
        assert!(list_pending(&db, "watcher").await.unwrap().is_empty());
        assert!(pending_at_boundary(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());

        // A second pass has nothing left to do: the row is no longer a
        // candidate, which is the whole point of writing the conclusion down.
        let verdicts = resolve_verdicts(&db, &[]).await.unwrap();
        assert!(verdicts.is_empty());
        assert_eq!(
            retire_terminal(&db, &[], &[]).await.unwrap(),
            0,
            "a settled recipient must perform no retirement writes"
        );
    }

    /// Each resolvable prefix retires under its own reason, so the audit trail
    /// says which referent resolved rather than merely that something did.
    #[tokio::test]
    async fn answered_questions_and_decided_permissions_retire_with_their_own_reasons() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "INSERT INTO prompts(id, run_id, questions, response, created_at)
               VALUES('q','run-1','[]','answered',1);
             INSERT INTO permission_requests
               (id, run_id, tool_use_id, tool_name, tool_input, status, created_at)
               VALUES('perm','run-1','tu','bash','{}','allowed',1);",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2/1/planner/questions/q-1",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            "permission:cairn://p/proj/2/1/builder/permissions/perm-1",
        )
        .await
        .unwrap();

        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert_eq!(
            count_where(&db, "retirement_reason='question_resolved'").await,
            1
        );
        assert_eq!(
            count_where(&db, "retirement_reason='permission_resolved'").await,
            1
        );
    }

    /// The distinction retirement exists to respect. None of these referents is
    /// deliverable, and none of them is terminal: an unanswered question is
    /// suppressed only by its issue's state, and a review push on an issue that
    /// produced neither a merge request nor a plan has nothing to prove
    /// resolution with. Retiring either would destroy an obligation that
    /// reopening the issue restores.
    #[tokio::test]
    async fn a_suppressed_or_unevidenced_referent_is_never_retired() {
        let db = migrated_db().await;
        seed(&db).await;
        db.execute_script(
            "INSERT INTO prompts(id, run_id, questions, response, created_at)
               VALUES('q','run-1','[]',NULL,1);
             UPDATE issues SET status='closed' WHERE id='issue-1';",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2/1/planner/questions/q-1",
        )
        .await
        .unwrap();
        // A review push whose issue carries no merge request and no plan.
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
        )
        .await
        .unwrap();

        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert_eq!(
            count_where(&db, "retired_at IS NOT NULL").await,
            0,
            "not deliverable is not the same as terminal"
        );
        assert_eq!(
            list_pending(&db, "watcher").await.unwrap().len(),
            2,
            "both must remain pending, so reopening the issue restores them"
        );
    }

    /// Fail-open cases must fail open in BOTH directions: still delivered, and
    /// never retired. Absence of understanding is not evidence of resolution.
    #[tokio::test]
    async fn informational_and_unresolvable_pushes_are_never_retired() {
        let db = migrated_db().await;
        seed(&db).await;
        for (key, content_ref) in [
            ("catchup:cairn://p/proj/2/1/planner", ISSUE_URI),
            ("direct:cairn://p/proj/2", ISSUE_URI),
            ("review:not-a-uri", "not-a-uri"),
            ("review:cairn://p/proj/9999", "cairn://p/proj/9999"),
        ] {
            push(
                &db,
                "watcher",
                content_ref,
                Wake::Wake,
                Boundary::Event,
                key,
            )
            .await
            .unwrap();
        }

        assert_eq!(
            list_pending_live(&db, "watcher").await.unwrap().len(),
            4,
            "informational, unparseable, and missing-issue pushes stay deliverable"
        );
        assert_eq!(count_where(&db, "retired_at IS NOT NULL").await, 0);
    }

    /// Delivery and retirement are mutually exclusive terminal writes, and the
    /// stamping guard is what keeps a retired row from being re-sealed as
    /// delivered by a drain that classified it before it was retired.
    #[tokio::test]
    async fn a_retired_row_cannot_be_stamped_delivered() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
        )
        .await
        .unwrap();
        let id = only_pending_id(&db).await;
        db.execute_script("UPDATE merge_requests SET status='merged' WHERE id='mr-open';")
            .await
            .unwrap();
        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());

        let stamped = stamp_delivered(&db, std::slice::from_ref(&id), "event-late")
            .await
            .unwrap();
        assert_eq!(stamped, 0, "a retired row is not deliverable");
        assert_eq!(count_where(&db, "delivered_event_id IS NOT NULL").await, 0);
        assert_eq!(
            retirement_reason_of(&db, &id).await.as_deref(),
            Some("review_resolved"),
            "a refused stamp must leave the retirement intact"
        );
    }

    /// Supersession is scoped to ACTIVE pending rows, while the fingerprint
    /// history spans everything. Together those two facts mean an unchanged
    /// source state stays deduplicated against retired history, and a genuinely
    /// changed one opens a fresh row instead of reviving a retired one.
    #[tokio::test]
    async fn a_changed_source_state_inserts_a_fresh_row_beside_retired_history() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        push_with_fingerprint(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
            Some("fp-A"),
        )
        .await
        .unwrap();
        let retired = only_pending_id(&db).await;
        db.execute_script("UPDATE merge_requests SET status='merged' WHERE id='mr-open';")
            .await
            .unwrap();
        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert!(retirement_reason_of(&db, &retired).await.is_some());

        // History is unbroken across retirement, so a creator that recomputes
        // the same fingerprint still knows not to re-fire.
        assert_eq!(
            latest_push_fingerprint(&db, "watcher", REVIEW_KEY)
                .await
                .unwrap(),
            Some(Some("fp-A".to_string())),
            "retirement must not erase the deduplication history"
        );

        // A new review on the same key -- the merge request is open again, so
        // there is something to look at once more. The retired row has left the
        // supersede index, so this inserts rather than reviving it in place.
        db.execute_script("UPDATE merge_requests SET status='open' WHERE id='mr-open';")
            .await
            .unwrap();
        push_with_fingerprint(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
            Some("fp-B"),
        )
        .await
        .unwrap();

        assert_eq!(
            count_where(&db, "1=1").await,
            2,
            "a fresh row, not a revival"
        );
        assert_eq!(
            retirement_reason_of(&db, &retired).await.as_deref(),
            Some("review_resolved"),
            "the retired row must stay retired"
        );
        let pending = list_pending_live(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_ne!(pending[0].id, retired);
    }

    /// The rerun case, and the one retirement is most dangerous in.
    ///
    /// An issue whose earlier run merged keeps that merged `merge_requests` row
    /// forever. So when the NEXT run writes its `create-pr` artifact and its
    /// review push, the issue simultaneously has a create-pr artifact and a
    /// non-open merge request -- which reads exactly like "the review has been
    /// had" unless the evidence is tied to a generation.
    ///
    /// Getting this wrong is worse than the bug retirement fixes. Previously
    /// this window was merely filtered and went live the moment the new PR row
    /// landed; retiring it makes the drop permanent and silent, which is
    /// CAIRN-2410 made durable.
    ///
    /// The push must survive the window as a PENDING row and become live when
    /// its own merge request opens -- the same row, not a replacement.
    #[tokio::test]
    async fn a_rerun_awaiting_its_pr_row_is_suspended_not_retired() {
        let db = migrated_db().await;
        seed(&db).await;
        // The previous generation: a merge request that has already merged.
        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-old','child-job','p','issue-1','t','b1','main','merged',10,10);
             INSERT INTO jobs(id, project_id, issue_id, execution_id, node_name, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES('child-job-2','p','issue-1','exec-1','builder','builder','running','sess3',100,100);",
        )
        .await
        .unwrap();

        // The rerun writes its create-pr artifact (auto-confirmed on write,
        // CAIRN-1219) and the review push. The new PR row has not landed yet.
        db.execute_script(
            "INSERT INTO artifacts
               (id, job_id, artifact_type, schema_version, data, version, output_name, confirmed, created_at, updated_at)
             VALUES('a-pr-2','child-job-2','create-pr',1,'{}',1,'create-pr',1,100,100);",
        )
        .await
        .unwrap();
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
        )
        .await
        .unwrap();
        let id = only_pending_id(&db).await;

        // Not deliverable during the window -- unchanged from before retirement
        // existed -- but emphatically not retired.
        assert!(
            list_pending_live(&db, "watcher").await.unwrap().is_empty(),
            "the pre-open window still reads not-live, exactly as before"
        );
        assert_eq!(
            retirement_reason_of(&db, &id).await,
            None,
            "the old generation's merged merge request is not evidence about \
             this push; retiring here would drop the rerun's review for good"
        );
        assert_eq!(list_pending(&db, "watcher").await.unwrap().len(), 1);

        // The rerun's own merge request opens.
        db.execute_script(
            "INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch, status, opened_at, updated_at)
             VALUES('mr-new','child-job-2','p','issue-1','t','b2','main','open',200,200);",
        )
        .await
        .unwrap();

        let live = list_pending_live(&db, "watcher").await.unwrap();
        assert_eq!(live.len(), 1, "the review becomes deliverable");
        assert_eq!(
            live[0].id, id,
            "the SAME row goes live -- it was never retired and nothing was \
             recreated in its place"
        );

        // And once that merge request merges, it finally retires.
        db.execute_script("UPDATE merge_requests SET status='merged' WHERE id='mr-new';")
            .await
            .unwrap();
        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert_eq!(
            retirement_reason_of(&db, &id).await.as_deref(),
            Some("review_resolved"),
            "the generation guard defers retirement, it does not prevent it"
        );
    }

    /// CAIRN-4181 proved a drain's cost does not grow with the number of LIVE
    /// pending pushes. This proves the other axis, which is the one retirement
    /// is about: it must not grow with accumulated HISTORY either. Otherwise the
    /// queue simply trades an unbounded pending set for an unbounded retired
    /// one and the per-tool-call cost creeps back.
    #[tokio::test]
    async fn drain_cost_is_bounded_by_live_rows_not_by_retired_history() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        push(
            &db,
            "watcher",
            ISSUE_URI,
            Wake::Wake,
            Boundary::Event,
            REVIEW_KEY,
        )
        .await
        .unwrap();

        let before = db.read_transaction_count();
        let fresh = pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        let without_history = db.read_transaction_count() - before;

        // A thousand retired rows for the same recipient: the production shape,
        // compressed. They are inserted already-retired because that is exactly
        // the state an aged install is in after the backfill has run.
        for n in 0..1000 {
            db.execute(
                "INSERT INTO attention_pushes
                   (id, recipient, content_ref, wake, boundary, key, created_at,
                    retired_at, retirement_reason)
                 VALUES('old-' || ?1, 'watcher', ?2, 'wake', 'event',
                        'review:old-' || ?1, 1, 2, 'review_resolved')",
                params![n as i64, ISSUE_URI],
            )
            .await
            .unwrap();
        }

        let before = db.read_transaction_count();
        let aged = pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        let with_history = db.read_transaction_count() - before;

        assert_eq!(
            fresh.len(),
            aged.len(),
            "retired history must not change what a drain returns"
        );
        assert_eq!(
            without_history, with_history,
            "a thousand retired rows must cost the same as none; {without_history} \
             vs {with_history} means history is being re-resolved"
        );
        assert_eq!(
            count_where(&db, "retired_at IS NOT NULL").await,
            1000,
            "the history is genuinely present, so the comparison means something"
        );
    }

    #[tokio::test]
    async fn pending_deliverable_live_includes_passive_excludes_turn_and_resolved() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        // wake + event + live referent -> drained at the event boundary.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        // passive + event -> NOW included: it rides along inline on the active
        // turn (wake level governs idle-waking, not the busy boundary drain).
        push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/proj/2/1/child",
        )
        .await
        .unwrap();
        // wake but turn boundary -> excluded: not an event-boundary push.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Turn,
            "review:turn",
        )
        .await
        .unwrap();
        // wake + event but referent resolved (no pending prompt for this question)
        // -> excluded: lazy_resolve drops a dead push.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2",
        )
        .await
        .unwrap();

        let drained = pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        let mut keys: Vec<&str> = drained.iter().map(|p| p.key.as_str()).collect();
        keys.sort_unstable();
        // Both live Event-boundary pushes, every wake level; Turn and resolved out.
        assert_eq!(
            keys,
            vec![
                "catchup:cairn://p/proj/2/1/child",
                "review:cairn://p/proj/2"
            ]
        );
        // Rendered into non-empty reminder lines for the agent.
        for p in &drained {
            assert!(!render_push(p).is_empty());
        }
    }

    #[tokio::test]
    async fn list_pending_live_includes_passive_excludes_resolved() {
        let db = migrated_db().await;
        seed(&db).await;
        // passive informational push: always live, rides along on resume.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/proj/2/1/child",
        )
        .await
        .unwrap();
        // review push with NO open MR -> referent resolved -> skipped at drain.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        let live = list_pending_live(&db, "watcher").await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].wake, Wake::Passive);
        assert_eq!(live[0].key, "catchup:cairn://p/proj/2/1/child");
    }

    #[tokio::test]
    async fn has_pending_waking_live_true_for_wake_false_for_passive() {
        let db = migrated_db().await;
        seed(&db).await;
        // Passive-only queue: never a reason to resume an idle agent.
        push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/proj/2/1/child",
        )
        .await
        .unwrap();
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());

        // A live wake push (any boundary) IS a reason to resume.
        open_mr(&db).await;
        push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Turn,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        assert!(has_pending_waking_live(&db, "watcher").await.unwrap());
    }

    /// The motivating CAIRN-2028 scenario: a passive `direct:` note (the clean
    /// auto-rebase notice) addressed to a busy recipient. It must drain at the
    /// recipient's next event boundary, yet never count as a reason to wake an
    /// idle agent. Both halves of the contract are locked here.
    #[tokio::test]
    async fn passive_direct_delivers_at_event_boundary_without_waking_idle() {
        let db = migrated_db().await;
        seed(&db).await;
        // Mirror insert_system_direct_push_conn: wake='passive', boundary='event',
        // key='direct:{id}'. A `direct:` referent is always live.
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Passive,
            Boundary::Event,
            "direct:msg-1",
        )
        .await
        .unwrap();

        // (a) A busy recipient drains it at the event boundary despite being passive.
        let drained = pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].key, "direct:msg-1");
        // (c) ...but it is never a reason to wake an idle agent.
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());

        // (b) Carrying event + stamp in one transaction marks it delivered.
        let pid = id.clone();
        db.write(|conn| {
            let pid = pid.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES('carry-direct','run-1',1,1,'system:message','{}',1)",
                    (),
                )
                .await?;
                let n = stamp_delivered_conn(conn, std::slice::from_ref(&pid), "carry-direct").await?;
                assert_eq!(n, 1);
                Ok::<(), DbError>(())
            })
        })
        .await
        .unwrap();

        assert_eq!(
            delivered_event(&db, &id).await,
            Some("carry-direct".to_string())
        );
        // Delivered row leaves the queue -> a second drain finds nothing.
        assert!(pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
        // (c) still holds after delivery: never woke an idle agent.
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());
    }

    #[tokio::test]
    async fn stamp_commits_atomically_with_carrying_event() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        // Unstamped until a carrying event exists.
        assert!(delivered_event(&db, &id).await.is_none());

        // Event INSERT + stamp in ONE transaction.
        let pid = id.clone();
        db.write(|conn| {
            let pid = pid.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES('carry-1','run-1',1,1,'system:message','{}',1)",
                    (),
                )
                .await?;
                let n = stamp_delivered_conn(conn, std::slice::from_ref(&pid), "carry-1").await?;
                assert_eq!(n, 1);
                Ok::<(), DbError>(())
            })
        })
        .await
        .unwrap();

        assert_eq!(delivered_event(&db, &id).await, Some("carry-1".to_string()));
        // Delivered row leaves the queue -> a second drain finds nothing.
        assert!(pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn rolled_back_carrying_event_leaves_push_pending() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        // Event INSERT + stamp, then force the transaction to roll back.
        let pid = id.clone();
        let res = db
            .write(|conn| {
                let pid = pid.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                         VALUES('carry-x','run-1',1,1,'system:message','{}',1)",
                        (),
                    )
                    .await?;
                    stamp_delivered_conn(conn, std::slice::from_ref(&pid), "carry-x").await?;
                    Err::<(), DbError>(DbError::Row("forced rollback".into()))
                })
            })
            .await;
        assert!(res.is_err());

        // Event and stamp roll back together: the push stays pending and redelivers.
        assert!(delivered_event(&db, &id).await.is_none());
        assert_eq!(list_pending_live(&db, "watcher").await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn second_drain_after_stamp_excludes_push() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();

        let first = pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap();
        assert_eq!(first.len(), 1);

        assert_eq!(stamp_delivered(&db, &[id], "carry-1").await.unwrap(), 1);

        // A second drain finds nothing to re-render or re-stamp.
        assert!(pending_deliverable_live(&db, "watcher", Boundary::Event)
            .await
            .unwrap()
            .is_empty());
        assert!(list_pending_live(&db, "watcher").await.unwrap().is_empty());
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());
    }

    // ---- Catch-up read cursors (CAIRN-1894) ----------------------------------

    /// Insert a chat event carrying `turn_id` on the child issue's run so
    /// `count_issue_chat_turns` sees a distinct turn.
    async fn add_chat_turn(db: &LocalDb, turn_id: &str, seq: i64) {
        let turn_id = turn_id.to_string();
        db.write(move |conn| {
            let turn_id = turn_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
                     VALUES(?1,'sess2','run-1',?2,'completed',1,1)",
                    params![turn_id.as_str(), seq],
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1',?2,?3,1,'assistant','{}',1)",
                    params![format!("ev-{turn_id}"), turn_id.as_str(), seq],
                )
                .await?;
                Ok::<(), DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// Insert a carrying event, stamp the pushes delivered, and advance their
    /// read cursors — all in one transaction, mirroring the real delivery seam.
    async fn deliver_advancing(db: &LocalDb, push_ids: &[String], event_id: &str, seq: i64) {
        let push_ids = push_ids.to_vec();
        let event_id = event_id.to_string();
        db.write(move |conn| {
            let push_ids = push_ids.clone();
            let event_id = event_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1',?2,1,'system:message','{}',1)",
                    params![event_id.as_str(), seq],
                )
                .await?;
                stamp_delivered_conn(conn, &push_ids, &event_id).await?;
                advance_read_cursors_conn(conn, &push_ids).await?;
                Ok::<(), DbError>(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn read_cursor_none_then_value_after_delivery() {
        let db = migrated_db().await;
        seed(&db).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            None
        );

        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child/chat?offset=0",
            Wake::Passive,
            Boundary::Event,
            "catchup:child-job",
        )
        .await
        .unwrap();
        deliver_advancing(&db, std::slice::from_ref(&id), "carry-1", 100).await;

        // Delivery advanced the cursor to the child's current tail (2 turns).
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(2)
        );
    }

    #[tokio::test]
    async fn advance_is_monotonic_and_ignores_non_catchup() {
        let db = migrated_db().await;
        seed(&db).await;
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        add_chat_turn(&db, "t3", 3).await;

        let (cid, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child/chat?offset=0",
            Wake::Passive,
            Boundary::Event,
            "catchup:child-job",
        )
        .await
        .unwrap();
        deliver_advancing(&db, std::slice::from_ref(&cid), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(3)
        );

        // A cursor already past the current tail must not rewind: MAX keeps it.
        db.execute_script(
            "UPDATE attention_read_cursors SET position=10 WHERE recipient='watcher';",
        )
        .await
        .unwrap();
        let (cid2, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2/1/child/chat?offset=3",
            Wake::Passive,
            Boundary::Event,
            "catchup:child-job",
        )
        .await
        .unwrap();
        deliver_advancing(&db, std::slice::from_ref(&cid2), "carry-2", 101).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(10),
            "advance must never rewind the cursor below its current value"
        );

        // A non-catchup push leaves cursors untouched.
        open_mr(&db).await;
        let (rid, _) = push(
            &db,
            "watcher",
            "cairn://p/proj/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/proj/2",
        )
        .await
        .unwrap();
        deliver_advancing(&db, std::slice::from_ref(&rid), "carry-3", 102).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(10),
            "a non-catchup push must not touch the read cursor"
        );
    }

    #[tokio::test]
    async fn mixed_case_uri_key_round_trips_through_store_and_match() {
        let db = migrated_db().await;
        seed(&db).await;
        let (first_id, _) = push_with_fingerprint(
            &db,
            "watcher",
            "cairn://p/CaIrN/42/1/Builder/artifact",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/CaIrN/42",
            Some("same-fact"),
        )
        .await
        .unwrap();
        assert_eq!(
            latest_push_fingerprint(&db, "watcher", "review:cairn://p/cairn/42")
                .await
                .unwrap(),
            Some(Some("same-fact".to_string()))
        );

        let (second_id, _) = push(
            &db,
            "watcher",
            "cairn://p/cairn/42/1/Builder/create-pr",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/cairn/42",
        )
        .await
        .unwrap();
        assert_eq!(first_id, second_id, "the canonical key supersedes in place");
        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].key, "review:cairn://p/cairn/42");
        assert_eq!(
            pending[0].content_ref,
            "cairn://p/cairn/42/1/Builder/create-pr"
        );
    }
}
