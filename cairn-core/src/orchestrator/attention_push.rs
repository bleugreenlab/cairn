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

use cairn_common::uri::parse_uri;
use turso::params;
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

fn now_ts() -> i64 {
    chrono::Utc::now().timestamp()
}

fn push_from_row(row: &turso::Row) -> DbResult<Push> {
    let wake = row.text(3)?;
    let boundary = row.text(4)?;
    Ok(Push {
        id: row.text(0)?,
        recipient: row.text(1)?,
        content_ref: row.text(2)?,
        wake: Wake::from_db(&wake)
            .ok_or_else(|| DbError::Row(format!("invalid push wake: {wake}")))?,
        boundary: Boundary::from_db(&boundary)
            .ok_or_else(|| DbError::Row(format!("invalid push boundary: {boundary}")))?,
        key: row.text(5)?,
        created_at: row.i64(6)?,
        delivered_event_id: row.opt_text(7)?,
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
        "resolved" => ("resolved", "Issue resolved"),
        "tasks" => ("tasks", "Tasks need attention"),
        "turn-checks" => ("checks", "Turn-end check results"),
        other => (other, "Attention update"),
    }
}

pub fn pushes_to_briefing_json(pushes: &[Push]) -> String {
    let mut active = Vec::new();
    let mut catchup = Vec::new();
    for push in pushes {
        let prefix = push
            .key
            .split_once(':')
            .map(|(p, _)| p)
            .unwrap_or(&push.key);
        let (kind, headline) = push_kind_headline(prefix);
        let item = serde_json::json!({
            "kind": kind,
            "headline": headline,
            "uri": push.content_ref,
        });
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

/// Downgrade an issue-sourced push (`review` / `question` / `permission`) to
/// `Passive` when the recipient holds an active **mute** on the subject issue.
/// This is the creation-time sibling of [`lazy_resolve_live`]'s drain-time issue
/// resolution: applying it centrally in [`push_with_fingerprint`] makes the
/// muted-source bug-class structural — no issue-push creator can forget to
/// consult mute. The subject issue URI is the push key's suffix
/// (`{prefix}:{issue_uri}`), which is exactly the `source_ref` an issue
/// subscription stores, so no DB lookup of the issue is needed. Non-`Wake`
/// levels (`Passive` already lowest, `Interrupt` never downgraded) and non-issue
/// prefixes (`catchup` / `resolved` / `direct`) short-circuit without a query.
/// A direct's source is its sender (peer/user axis), not the subject URI, so the
/// direct creator applies the same [`crate::orchestrator::wakes::mute_downgrade`]
/// rule explicitly at its own site rather than here.
async fn issue_mute_downgrade(
    db: &LocalDb,
    recipient: &str,
    key: &str,
    requested: Wake,
) -> DbResult<Wake> {
    if requested != Wake::Wake {
        return Ok(requested);
    }
    let Some((prefix, issue_uri)) = key.split_once(':') else {
        return Ok(requested);
    };
    if !matches!(prefix, "review" | "question" | "permission") {
        return Ok(requested);
    }
    crate::orchestrator::wakes::mute_downgrade(
        db,
        recipient,
        "issue",
        Some(issue_uri),
        prefix,
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
    // Consult mute centrally for issue-sourced prefixes; a muted source's `Wake`
    // becomes `Passive` so the row is created as a ride-along rather than a
    // rousing wake (CAIRN-1900). The effective wake is returned so the caller
    // skips nudging a downgraded recipient.
    let wake = issue_mute_downgrade(db, recipient, key, wake).await?;
    let recipient = recipient.to_string();
    let content_ref = content_ref.to_string();
    let key = key.to_string();
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
                     WHERE recipient=?6 AND key=?7 AND delivered_event_id IS NULL",
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
                     WHERE recipient=?1 AND key=?2 AND delivered_event_id IS NULL
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
/// `(recipient, key)`, newest by `created_at`. Outer `None` = no such push
/// exists; `Some(None)` = a push with a NULL fingerprint. The review creator
/// uses this to skip re-firing a review when the reviewable state is unchanged
/// since the last review push to the recipient (CAIRN-1889).
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
                     ORDER BY created_at DESC, id DESC
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
                     WHERE recipient=?1 AND delivered_event_id IS NULL
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
                     WHERE recipient=?1 AND delivered_event_id IS NULL
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
    let pending = list_pending(db, recipient).await?;
    let mut live = Vec::with_capacity(pending.len());
    for push in pending {
        if lazy_resolve_live(db, &push).await? {
            live.push(push);
        }
    }
    Ok(live)
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
    let pending = pending_at_boundary(db, recipient, boundary).await?;
    let mut live = Vec::with_capacity(pending.len());
    for push in pending {
        if lazy_resolve_live(db, &push).await? {
            live.push(push);
        }
    }
    Ok(live)
}

/// Whether the recipient has any undelivered *rousing* (`wake`/`interrupt`) push
/// that is still live, regardless of boundary. The idle-flush resume gate's
/// predicate: a rousing push is a reason to wake an idle agent. `passive` pushes
/// are excluded by construction, so they never wake — they only ride along on a
/// resume that happens for some other reason (drained by [`list_pending_live`]).
pub async fn has_pending_waking_live(db: &LocalDb, recipient: &str) -> DbResult<bool> {
    let pending = list_pending(db, recipient).await?;
    for push in pending {
        if push.wake == Wake::Passive {
            continue;
        }
        if lazy_resolve_live(db, &push).await? {
            return Ok(true);
        }
    }
    Ok(false)
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
                "DELETE FROM attention_pushes WHERE id = ?1 AND delivered_event_id IS NULL",
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
    conn: &turso::Connection,
    push_ids: &[String],
    event_id: &str,
) -> DbResult<usize> {
    let mut stamped = 0usize;
    for id in push_ids {
        let affected = conn
            .execute(
                "UPDATE attention_pushes SET delivered_event_id=?1
                 WHERE id=?2 AND delivered_event_id IS NULL",
                params![event_id, id.as_str()],
            )
            .await?;
        stamped += affected as usize;
    }
    Ok(stamped)
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

/// Distinct chat turns currently recorded across one job's runs — the job-scoped
/// chat tail, matching exactly what `{node}/chat` renders (the node chat loads
/// events for that one job's runs). Runs on a caller-supplied connection so the
/// catch-up cursor advance can compute the delivery-time tail inside the stamp
/// transaction.
async fn count_job_chat_turns(conn: &turso::Connection, job_id: &str) -> DbResult<i64> {
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
    conn: &turso::Connection,
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
    let prefix = push
        .key
        .split_once(':')
        .map(|(p, _)| p)
        .unwrap_or(&push.key);
    let referent = match prefix {
        "review" | "question" | "permission" => prefix.to_string(),
        _ => return Ok(true),
    };
    // Resolve the subject issue from the content_ref URI.
    let Some(parsed) = parse_uri(&push.content_ref) else {
        return Ok(true); // unparseable ref -> don't silently drop
    };
    let project = parsed.project().map(str::to_uppercase);
    let number = parsed.issue_number();
    let (Some(project_key), Some(number)) = (project, number) else {
        return Ok(true);
    };
    db.read(|conn| {
        let project_key = project_key.clone();
        let referent = referent.clone();
        Box::pin(async move {
            let Some(issue_id) = lookup_issue_id(conn, &project_key, number).await? else {
                return Ok(true); // issue not found -> don't drop
            };
            let live = match referent.as_str() {
                // Live while there is reviewable output: an open unmerged PR OR
                // an unconfirmed create-pr/plan artifact for the issue. The
                // second arm is load-bearing for a plan-review push, which never
                // has a PR row.
                "review" => {
                    exists(
                        conn,
                        "SELECT 1 FROM merge_requests
                         WHERE issue_id=?1 AND status NOT IN ('merged','closed') LIMIT 1",
                        &issue_id,
                    )
                    .await?
                        || exists(
                            conn,
                            "SELECT 1 FROM artifacts a JOIN jobs j ON a.job_id=j.id
                             WHERE j.issue_id=?1
                               AND a.artifact_type IN ('create-pr','plan')
                               AND a.confirmed=0 LIMIT 1",
                            &issue_id,
                        )
                        .await?
                }
                // Live only while the referent is still pending AND the subject
                // issue is not terminal: a blocker on a closed/merged/failed
                // issue is dead even if its prompts/permission_requests row was
                // never resolved.
                "question" => {
                    !issue_is_terminal(conn, &issue_id).await?
                        && exists(
                            conn,
                            "SELECT 1 FROM prompts p JOIN runs r ON p.run_id=r.id
                             WHERE r.issue_id=?1 AND p.response IS NULL LIMIT 1",
                            &issue_id,
                        )
                        .await?
                }
                "permission" => {
                    !issue_is_terminal(conn, &issue_id).await?
                        && exists(
                            conn,
                            "SELECT 1 FROM permission_requests pr JOIN runs r ON pr.run_id=r.id
                             WHERE r.issue_id=?1 AND pr.status='pending' LIMIT 1",
                            &issue_id,
                        )
                        .await?
                }
                _ => true,
            };
            Ok::<bool, DbError>(live)
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

async fn lookup_issue_id(
    conn: &turso::Connection,
    project_key: &str,
    number: i32,
) -> DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT i.id FROM issues i JOIN projects p ON p.id=i.project_id
             WHERE p.key=?1 AND i.number=?2 LIMIT 1",
            params![project_key, number as i64],
        )
        .await?;
    match rows.next().await? {
        Some(row) => Ok(Some(row.text(0)?)),
        None => Ok(None),
    }
}

async fn exists(conn: &turso::Connection, sql: &str, issue_id: &str) -> DbResult<bool> {
    let mut rows = conn.query(sql, params![issue_id]).await?;
    Ok(rows.next().await?.is_some())
}

/// Whether the issue is terminal (`merged`/`closed`/`failed`), mirroring
/// [`crate::models::IssueStatus::is_terminal`]. A `question:`/`permission:` push
/// for a terminalized issue is dead — the blocker no longer needs a watcher.
async fn issue_is_terminal(conn: &turso::Connection, issue_id: &str) -> DbResult<bool> {
    let mut rows = conn
        .query("SELECT status FROM issues WHERE id=?1", params![issue_id])
        .await?;
    match rows.next().await? {
        Some(row) => Ok(matches!(
            row.text(0)?.as_str(),
            "merged" | "closed" | "failed"
        )),
        None => Ok(false),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    const ISSUE_URI: &str = "cairn://p/PROJ/2";

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-push.db").await
    }

    /// Seed a project, an issue (`issue-1` / `cairn://p/PROJ/2`), a watcher job
    /// (the recipient), a child job, and a run for that issue so the FK and the
    /// referent-table resolution queries have rows to work against.
    async fn seed(db: &LocalDb) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','issue-1','running','sess',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','running','sess2',1,1);
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
    async fn push_inserts_a_pending_row() {
        let db = migrated_db().await;
        seed(&db).await;
        let (id, _) = push(
            &db,
            "watcher",
            "cairn://p/PROJ/2/1/builder",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
        )
        .await
        .unwrap();

        let pending = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, id);
        assert_eq!(pending[0].recipient, "watcher");
        assert_eq!(pending[0].wake, Wake::Wake);
        assert_eq!(pending[0].boundary, Boundary::Event);
        assert_eq!(pending[0].key, "review:cairn://p/PROJ/2");
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
            "review:cairn://p/PROJ/2",
        )
        .await
        .unwrap();
        let (second, _) = push(
            &db,
            "watcher",
            "ref-2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2/1/builder",
            Wake::Wake,
            Boundary::Event,
            "direct:msg-1",
        )
        .await
        .unwrap();
        let (id2, w2) = push(
            &db,
            "watcher",
            "cairn://p/PROJ/2/1/builder",
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
            "review:cairn://p/PROJ/2",
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
            "review:cairn://p/PROJ/2",
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
    async fn lazy_resolve_review_lives_with_open_pr_skips_when_merged() {
        let db = migrated_db().await;
        seed(&db).await;
        let p = sample_push("review:cairn://p/PROJ/2", "cairn://p/PROJ/2/1/builder");

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
            "question:cairn://p/PROJ/2/1/planner/questions/q-1",
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
            "permission:cairn://p/PROJ/2/1/builder/permissions/perm-1",
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
            "question:cairn://p/PROJ/2/1/planner/questions/q-1",
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
        let p = sample_push("review:cairn://p/PROJ/2", "cairn://p/PROJ/2/1/planner/plan");
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
    async fn lazy_resolve_informational_prefixes_are_always_live() {
        let db = migrated_db().await;
        seed(&db).await;
        for key in [
            "catchup:cairn://p/PROJ/2/1/child",
            "direct:cairn://p/PROJ/2",
            "resolved:cairn://p/PROJ/2",
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

    #[tokio::test]
    async fn pending_deliverable_live_includes_passive_excludes_turn_and_resolved() {
        let db = migrated_db().await;
        seed(&db).await;
        open_mr(&db).await;
        // wake + event + live referent -> drained at the event boundary.
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
        )
        .await
        .unwrap();
        // passive + event -> NOW included: it rides along inline on the active
        // turn (wake level governs idle-waking, not the busy boundary drain).
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/PROJ/2/1/child",
        )
        .await
        .unwrap();
        // wake but turn boundary -> excluded: not an event-boundary push.
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/PROJ/2",
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
                "catchup:cairn://p/PROJ/2/1/child",
                "review:cairn://p/PROJ/2"
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
            "cairn://p/PROJ/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/PROJ/2/1/child",
        )
        .await
        .unwrap();
        // review push with NO open MR -> referent resolved -> skipped at drain.
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
        )
        .await
        .unwrap();

        let live = list_pending_live(&db, "watcher").await.unwrap();
        assert_eq!(live.len(), 1);
        assert_eq!(live[0].wake, Wake::Passive);
        assert_eq!(live[0].key, "catchup:cairn://p/PROJ/2/1/child");
    }

    #[tokio::test]
    async fn has_pending_waking_live_true_for_wake_false_for_passive() {
        let db = migrated_db().await;
        seed(&db).await;
        // Passive-only queue: never a reason to resume an idle agent.
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2/1/child",
            Wake::Passive,
            Boundary::Event,
            "catchup:cairn://p/PROJ/2/1/child",
        )
        .await
        .unwrap();
        assert!(!has_pending_waking_live(&db, "watcher").await.unwrap());

        // A live wake push (any boundary) IS a reason to resume.
        open_mr(&db).await;
        push(
            &db,
            "watcher",
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Turn,
            "review:cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
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
            "cairn://p/PROJ/2/1/child/chat?offset=0",
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
            "cairn://p/PROJ/2/1/child/chat?offset=0",
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
            "cairn://p/PROJ/2/1/child/chat?offset=3",
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
            "cairn://p/PROJ/2",
            Wake::Wake,
            Boundary::Event,
            "review:cairn://p/PROJ/2",
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
}
