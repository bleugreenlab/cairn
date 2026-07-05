//! `watch` — block until an issue needs attention or is done, without polling.
//!
//! The external driving loop calls `watch <issue-uri> [--since <updated_at>]`.
//! The handler:
//! 1. Resolves the issue and reads its attention + status projection +
//!    `updated_at`.
//! 2. Current-state check: if already actionable and newer than `since`, or in
//!    a terminal status, returns immediately, synthesizing an event from the
//!    live projection (this closes the gap between successive calls).
//! 3. Otherwise subscribes to `orch.attention_changed` and awaits a typed
//!    `AttentionEvent` for this issue, returning when a matching one arrives.
//!    The response is built from the event content — no follow-up `read`
//!    round-trip and no projection re-derivation on the wake.
//!
//! Two return shapes end the loop. `actionable` means the issue needs the
//! driver (a question, a gated artifact, a review). `resolved` means the issue
//! reached a terminal status (merged/closed/failed) — there is no more work, so
//! the driver stops rather than waiting forever on a done issue. Terminal
//! short-circuits the `--since` cursor: a finished issue should never block.
//!
//! The `--since` cursor (the last-seen `updated_at`) plus the step-1
//! current-state check is the non-polling keystone: a change that fires between
//! calls is caught by the next call's check rather than missed, so the harness
//! never needs a timed poll to stay correct.

use cairn_db::turso::params;
use serde_json::json;

use crate::mcp::types::McpCallbackRequest;
use crate::models::{IssueAttention, IssueStatus};
use crate::orchestrator::attention::{
    event_to_watch_json, idle_fact_for_issue, AttentionEvent, AttentionFact,
    ExternalMessageReplyContent, IssueAttentionContext,
};
use crate::orchestrator::Orchestrator;
use crate::storage::{DbError, LocalDb, RowExt};

/// Server-side long-poll budget. Comfortably under the CLI's 600s client
/// timeout (mirrors the permission-prompt await convention). On expiry the
/// handler returns a `pending` sentinel and the CLI re-issues with an updated
/// cursor — one continuous `cairn watch` to the caller.
const SERVER_WATCH_BUDGET: std::time::Duration = std::time::Duration::from_secs(290);

struct IssueRef {
    project_key: String,
    number: i32,
    issue_id: String,
}

fn error_json(message: &str) -> String {
    json!({ "status": "error", "error": message }).to_string()
}

/// `watch` returns iff the issue is actionable (attention != None) and that
/// state is newer than the caller's cursor.
fn is_actionable(attention: &IssueAttention, updated_at: i64, since: Option<i64>) -> bool {
    *attention != IssueAttention::None && since.is_none_or(|s| updated_at > s)
}

/// Whether a typed broadcast event should end the watch, given the caller's
/// cursor. This is distinct from [`is_actionable`], which gates on the
/// *attention projection*: a typed fact can be actionable even when attention
/// is `None`.
///
/// - `Resolved` always wins (a terminal issue must never block).
/// - `AgentIdleWithWork` is actionable on a fresh-vs-cursor `updated_at` even
///   when `attention` is `None` — the agent went idle leaving an open PR the
///   driver must act on, which the attention projection deliberately leaves as
///   `None` (no desktop badge for a fresh PR with unknown GitHub state).
/// - All other facts gate on the attention projection as before.
fn event_is_actionable(event: &AttentionEvent, since: Option<i64>) -> bool {
    match &event.fact {
        AttentionFact::Resolved { .. } => true,
        // A fresh-vs-cursor idle-with-work fact is actionable regardless of the
        // attention projection: the agent went idle leaving an open PR the driver
        // must act on, which the projection deliberately leaves as `None` (no
        // desktop badge for a fresh PR with unknown GitHub state).
        AttentionFact::AgentIdleWithWork { .. } | AttentionFact::ExternalMessageReply { .. } => {
            since.is_none_or(|s| event.updated_at > s)
        }
        _ => is_actionable(&event.attention, event.updated_at, since),
    }
}

/// What a single look at the issue's projection tells `watch` to do.
/// The actionable/resolved fields are not retained on the outcome itself;
/// callers synthesize the response from the live projection passed alongside.
enum WatchOutcome {
    /// Terminal status — the issue is done; stop the loop. Checked first and
    /// unconditional of `since`: blocking on a finished issue is never right.
    Resolved,
    /// Needs the driver now (a fresh, newer-than-cursor attention state).
    Actionable,
    /// Nothing yet — keep waiting.
    NotYet,
}

/// Decide from one `(attention, status, updated_at)` read. Terminal status wins
/// over attention: a merged issue with a stray attention value should still end
/// the watch as resolved, not loop on the (now moot) attention.
fn evaluate(
    attention: IssueAttention,
    status: IssueStatus,
    updated_at: i64,
    since: Option<i64>,
) -> WatchOutcome {
    if status.is_terminal() {
        WatchOutcome::Resolved
    } else if is_actionable(&attention, updated_at, since) {
        WatchOutcome::Actionable
    } else {
        WatchOutcome::NotYet
    }
}

async fn resolve_issue_ref(
    db: &LocalDb,
    project_key: &str,
    number: i32,
) -> Result<IssueRef, String> {
    let key = project_key.to_uppercase();
    let resolved = db
        .read(|conn| {
            let key = key.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.id FROM issues i JOIN projects p ON i.project_id = p.id
                         WHERE p.key = ?1 AND i.number = ?2 LIMIT 1",
                        params![key.as_str(), number],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some(row.text(0)?)),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    let issue_id = resolved.ok_or_else(|| format!("Issue {}-{} not found", key, number))?;
    Ok(IssueRef {
        project_key: key,
        number,
        issue_id,
    })
}

/// Read `(attention, status, updated_at)` for an issue.
async fn read_issue_state(
    db: &LocalDb,
    issue_id: &str,
) -> Result<(IssueAttention, IssueStatus, i64), String> {
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT attention, status, updated_at FROM issues WHERE id = ?1",
                    (issue_id.as_str(),),
                )
                .await?;
            let row = rows
                .next()
                .await?
                .ok_or_else(|| DbError::internal("issue not found"))?;
            let attention = row
                .text(0)?
                .parse::<IssueAttention>()
                .unwrap_or(IssueAttention::None);
            let status = row
                .text(1)?
                .parse::<IssueStatus>()
                .unwrap_or(IssueStatus::Backlog);
            let updated_at = row.opt_i64(2)?.unwrap_or(0);
            Ok::<_, DbError>((attention, status, updated_at))
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Non-blocking drain: if a typed event for `target` is already buffered on
/// the subscriber and passes the cursor gate, render it. Returns `None` when
/// nothing matches; the caller then synthesizes from the live projection.
/// Drops other-issue events so they don't block a relevant one further back.
fn try_pop_actionable_for(
    rx: &mut tokio::sync::broadcast::Receiver<AttentionEvent>,
    target: &str,
    since: Option<i64>,
) -> Option<String> {
    use tokio::sync::broadcast::error::TryRecvError;
    loop {
        match rx.try_recv() {
            Ok(event) => {
                if event.issue_id != target {
                    continue;
                }
                if event_is_actionable(&event, since) {
                    return Some(event_to_watch_json(&event).to_string());
                }
            }
            Err(TryRecvError::Empty) | Err(TryRecvError::Closed) | Err(TryRecvError::Lagged(_)) => {
                return None
            }
        }
    }
}

/// Build a synthetic event from the live projection — used by the step-1
/// catch-up read when no typed event is buffered, and by the `Lagged` recovery
/// path. Routes through the shared [`idle_fact_for_issue`] so the synthesized
/// fact's detail URI matches the live broadcast exactly (PR-state attention
/// resolves to the `/pr` resource, not the bare issue URI). Only reached when
/// `evaluate` is Resolved/Actionable (terminal or attention != none), so the
/// helper yields `Some`; the fallback keeps the function total.
async fn synthesize_event(
    db: &LocalDb,
    issue_ref: &IssueRef,
    attention: IssueAttention,
    status: IssueStatus,
    updated_at: i64,
) -> AttentionEvent {
    let ctx = IssueAttentionContext {
        project_key: issue_ref.project_key.clone(),
        number: issue_ref.number,
        attention: attention.clone(),
        status: status.clone(),
        updated_at,
    };
    let issue_uri = ctx.issue_uri();
    let fact = idle_fact_for_issue(db, &issue_ref.issue_id, &ctx, None)
        .await
        .map(|idle| idle.fact)
        .unwrap_or_else(|| AttentionFact::AgentIdleWithWork {
            detail_uri: issue_uri.clone(),
        });
    AttentionEvent {
        issue_id: issue_ref.issue_id.clone(),
        issue_uri,
        fact,
        attention,
        status,
        updated_at,
    }
}

async fn latest_external_reply_event(
    db: &LocalDb,
    issue_ref: &IssueRef,
    since: Option<i64>,
) -> Result<Option<AttentionEvent>, String> {
    let channel_id = format!("{}/{}", issue_ref.project_key, issue_ref.number);
    let sender_prefix = format!("cairn://p/{}/{}/", issue_ref.project_key, issue_ref.number);
    let issue_id = issue_ref.issue_id.clone();
    let row = db
        .read(|conn| {
            let channel_id = channel_id.clone();
            let sender_prefix = sender_prefix.clone();
            let issue_id = issue_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT m.id, m.sender_name, m.content, m.created_at,
                                i.attention, i.status
                         FROM messages m
                         JOIN issues i ON i.id = ?4
                         WHERE m.channel_type = 'issue'
                           AND m.channel_id = ?1
                           AND m.sender_run_id IS NOT NULL
                           AND m.sender_name LIKE ?2
                           AND (?3 IS NULL OR m.created_at > ?3)
                         ORDER BY m.created_at DESC
                         LIMIT 1",
                        params![
                            channel_id.as_str(),
                            format!("{}%", sender_prefix).as_str(),
                            since,
                            issue_id.as_str()
                        ],
                    )
                    .await?;
                match rows.next().await? {
                    Some(row) => Ok::<_, DbError>(Some((
                        row.text(0)?,
                        row.text(1)?,
                        row.text(2)?,
                        row.i64(3)?,
                        row.text(4)?,
                        row.text(5)?,
                    ))),
                    None => Ok(None),
                }
            })
        })
        .await
        .map_err(|e| e.to_string())?;

    let Some((message_id, sender, body, updated_at, attention, status)) = row else {
        return Ok(None);
    };
    let attention = attention
        .parse::<IssueAttention>()
        .unwrap_or(IssueAttention::None);
    let status = status.parse::<IssueStatus>().unwrap_or(IssueStatus::Active);
    let issue_uri = format!("cairn://p/{}/{}", issue_ref.project_key, issue_ref.number);
    let detail_uri = format!("{issue_uri}/messages");
    Ok(Some(AttentionEvent {
        issue_id: issue_ref.issue_id.clone(),
        issue_uri,
        fact: AttentionFact::ExternalMessageReply {
            detail_uri,
            message_id,
            content: ExternalMessageReplyContent { sender, body },
        },
        attention,
        status,
        updated_at,
    }))
}

/// Handle a `watch` tool call. Long-polls until the issue is actionable.
pub async fn handle_watch(orch: &Orchestrator, request: &McpCallbackRequest) -> String {
    let Some(issue_uri) = request.payload.get("issue_uri").and_then(|v| v.as_str()) else {
        return error_json("watch requires payload.issue_uri");
    };
    let since = request.payload.get("since").and_then(|v| v.as_i64());

    let parsed = cairn_common::uri::parse_uri(issue_uri).and_then(|resource| {
        match (
            resource.project().map(str::to_string),
            resource.issue_number(),
        ) {
            (Some(project), Some(number)) => Some((project, number)),
            _ => None,
        }
    });
    let Some((project_key, number)) = parsed else {
        return error_json(&format!("Not an issue URI: {issue_uri}"));
    };

    let issue_ref = match resolve_issue_ref(&orch.db.local, &project_key, number).await {
        Ok(r) => r,
        Err(e) => return error_json(&e),
    };

    // Subscribe BEFORE the catch-up read. The read has awaits after its SELECT
    // runs, and `watch` is the one broadcast-await handler whose event source
    // is fully concurrent (agents, webhooks, terminal resolve). Subscribing
    // first means an emit that fires during the read is buffered and delivered
    // to the loop below rather than dropped — closing the within-call twin of
    // the between-calls gap the cursor handles.
    let mut rx = orch.attention_changed.subscribe();

    // Step 1: current-state check (catch-up between calls). Distinguish a real
    // read error here, before committing to the long-poll.
    match read_issue_state(&orch.db.local, &issue_ref.issue_id).await {
        Ok((attention, status, updated_at)) => {
            match evaluate(attention.clone(), status.clone(), updated_at, since) {
                WatchOutcome::Resolved | WatchOutcome::Actionable => {
                    // Prefer a buffered typed event over a synthesized one:
                    // the buffered event carries inline content (question
                    // text, artifact title/summary, PR state, ...) that the
                    // projection-based synthesis cannot reconstruct. Drain
                    // the broadcast non-blockingly for a matching, on-cursor
                    // event; fall back to `synthesize_event` only when none
                    // is queued. This preserves the no-follow-up-read promise
                    // across the between-calls gap that the cursor handles.
                    if let Some(json) = try_pop_actionable_for(&mut rx, &issue_ref.issue_id, since)
                    {
                        return json;
                    }
                    let event =
                        synthesize_event(&orch.db.local, &issue_ref, attention, status, updated_at)
                            .await;
                    return event_to_watch_json(&event).to_string();
                }
                WatchOutcome::NotYet => {
                    if let Ok(Some(event)) =
                        latest_external_reply_event(&orch.db.local, &issue_ref, since).await
                    {
                        if event_is_actionable(&event, since) {
                            if let Some(json) =
                                try_pop_actionable_for(&mut rx, &issue_ref.issue_id, since)
                            {
                                return json;
                            }
                            return event_to_watch_json(&event).to_string();
                        }
                    }
                    // The issue projection isn't actionable, but a freshly-opened
                    // PR (GitHub state unknown, so attention is deliberately None)
                    // is actionable work. Consult the shared idle path directly —
                    // not `synthesize_event`, whose issue-URI fallback would
                    // fabricate a spurious wake for a plain idle bump. Return only
                    // a real fact (an open PR) that clears the caller's cursor.
                    let ctx = IssueAttentionContext {
                        project_key: issue_ref.project_key.clone(),
                        number: issue_ref.number,
                        attention,
                        status,
                        updated_at,
                    };
                    if let Some(idle) =
                        idle_fact_for_issue(&orch.db.local, &issue_ref.issue_id, &ctx, None).await
                    {
                        let event = AttentionEvent {
                            issue_id: issue_ref.issue_id.clone(),
                            issue_uri: ctx.issue_uri(),
                            fact: idle.fact,
                            attention: ctx.attention,
                            status: ctx.status,
                            updated_at: idle.updated_at,
                        };
                        if event_is_actionable(&event, since) {
                            if let Some(json) =
                                try_pop_actionable_for(&mut rx, &issue_ref.issue_id, since)
                            {
                                return json;
                            }
                            return event_to_watch_json(&event).to_string();
                        }
                    }
                }
            }
        }
        Err(e) => return error_json(&e),
    }

    // Step 2: await typed events for this issue — no polling. On match, render
    // the response directly from the event content (no follow-up DB read).
    let target = issue_ref.issue_id.clone();
    let db = &orch.db.local;
    let result = tokio::time::timeout(SERVER_WATCH_BUDGET, async {
        loop {
            match rx.recv().await {
                Ok(event) => {
                    if event.issue_id != target {
                        continue;
                    }
                    // Filter by cursor: a stale-vs-cursor event (no newer
                    // updated_at) is not what the caller is waiting for.
                    // Terminal facts always win; other facts follow the shared
                    // attention projection.
                    if event_is_actionable(&event, since) {
                        return Some(event_to_watch_json(&event).to_string());
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // Missed events; re-read once rather than assume nothing
                    // changed and synthesize an event from the live projection.
                    if let Ok(Some(event)) =
                        latest_external_reply_event(db, &issue_ref, since).await
                    {
                        return Some(event_to_watch_json(&event).to_string());
                    }
                    if let Ok((attention, status, updated_at)) = read_issue_state(db, &target).await
                    {
                        match evaluate(attention.clone(), status.clone(), updated_at, since) {
                            WatchOutcome::NotYet => {}
                            _ => {
                                let event =
                                    synthesize_event(db, &issue_ref, attention, status, updated_at)
                                        .await;
                                return Some(event_to_watch_json(&event).to_string());
                            }
                        }
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
            }
        }
    })
    .await;

    match result {
        Ok(Some(json)) => json,
        Ok(None) | Err(_) => {
            // Budget expired or channel closed: pending sentinel carrying the
            // current cursor so the CLI can re-issue with an accurate --since.
            let updated_at = read_issue_state(&orch.db.local, &issue_ref.issue_id)
                .await
                .map(|(_, _, u)| u)
                .unwrap_or(0);
            json!({ "status": "pending", "updated_at": updated_at }).to_string()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_attention_is_never_actionable() {
        assert!(!is_actionable(&IssueAttention::None, 100, None));
        assert!(!is_actionable(&IssueAttention::None, 100, Some(50)));
    }

    #[test]
    fn actionable_without_cursor_returns_on_any_attention() {
        assert!(is_actionable(&IssueAttention::NeedsInput, 100, None));
        assert!(is_actionable(&IssueAttention::NeedsApproval, 0, None));
    }

    #[test]
    fn cursor_gates_on_strictly_newer_updated_at() {
        // Same-or-older than the cursor: already seen, do not return.
        assert!(!is_actionable(&IssueAttention::NeedsInput, 100, Some(100)));
        assert!(!is_actionable(&IssueAttention::NeedsInput, 90, Some(100)));
        // Strictly newer: a fresh transition, return.
        assert!(is_actionable(&IssueAttention::NeedsInput, 101, Some(100)));
    }

    #[test]
    fn only_merged_closed_failed_are_terminal() {
        assert!(IssueStatus::Merged.is_terminal());
        assert!(IssueStatus::Closed.is_terminal());
        assert!(IssueStatus::Failed.is_terminal());
        // Transient/in-progress states keep the watch waiting.
        assert!(!IssueStatus::Backlog.is_terminal());
        assert!(!IssueStatus::Active.is_terminal());
        assert!(!IssueStatus::Waiting.is_terminal());
        // Complete is a successful-but-transient state (typically advances to a
        // PR/merge), so it is deliberately NOT terminal.
        assert!(!IssueStatus::Complete.is_terminal());
    }

    #[test]
    fn terminal_status_resolves_regardless_of_cursor() {
        // A merged issue ends the watch even when the cursor is at/after its
        // updated_at — blocking on a finished issue is never right.
        assert!(matches!(
            evaluate(IssueAttention::None, IssueStatus::Merged, 100, Some(100)),
            WatchOutcome::Resolved
        ));
        assert!(matches!(
            evaluate(IssueAttention::None, IssueStatus::Closed, 50, Some(999)),
            WatchOutcome::Resolved
        ));
    }

    #[test]
    fn terminal_status_wins_over_a_stray_attention() {
        assert!(matches!(
            evaluate(
                IssueAttention::NeedsApproval,
                IssueStatus::Merged,
                100,
                None
            ),
            WatchOutcome::Resolved
        ));
    }

    #[test]
    fn external_reply_event_is_actionable_without_projection_attention() {
        let event = AttentionEvent {
            issue_id: "i-1".to_string(),
            issue_uri: "cairn://p/CAIRN/1".to_string(),
            fact: AttentionFact::ExternalMessageReply {
                detail_uri: "cairn://p/CAIRN/1/messages".to_string(),
                message_id: "m-1".to_string(),
                content: ExternalMessageReplyContent {
                    sender: "cairn://p/CAIRN/1/1/builder".to_string(),
                    body: "done".to_string(),
                },
            },
            attention: IssueAttention::None,
            status: IssueStatus::Active,
            updated_at: 101,
        };

        assert!(event_is_actionable(&event, Some(100)));
        assert!(!event_is_actionable(&event, Some(101)));
    }

    use crate::storage::{MigrationRunner, TURSO_MIGRATIONS};
    use tempfile::tempdir;

    async fn test_db() -> LocalDb {
        let temp = tempdir().unwrap();
        let db = LocalDb::open(temp.path().join("watch.db")).await.unwrap();
        MigrationRunner::new(TURSO_MIGRATIONS.to_vec())
            .run(&db)
            .await
            .unwrap();
        db
    }

    fn test_issue_ref() -> IssueRef {
        IssueRef {
            project_key: "CAIRN".to_string(),
            number: 1181,
            issue_id: "i-1181".to_string(),
        }
    }

    async fn assert_waiting_approval_detail_uri(db: &LocalDb, expected: &str) {
        let event = synthesize_event(
            db,
            &test_issue_ref(),
            IssueAttention::NeedsApproval,
            IssueStatus::Waiting,
            1,
        )
        .await;
        match event.fact {
            AttentionFact::AgentIdleWithWork { detail_uri, .. } => {
                assert_eq!(detail_uri, expected);
            }
            other => panic!("expected AgentIdleWithWork, got {:?}", other),
        }
    }

    /// Seed a project + issue + execution + a single blocked node job.
    async fn seed_blocked_node(db: &LocalDb, artifact_name: Option<&str>) {
        let artifact_name = artifact_name.map(str::to_string);
        db.write(move |conn| {
            let artifact_name = artifact_name.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1', 'W', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p-cairn', 'w-1', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                     VALUES ('i-1181', 'p-cairn', 1181, 'T', '', 'waiting', 'waiting', 'needs_approval', 0, 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('e-1', 'default', 'i-1181', 'p-cairn', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO jobs (id, execution_id, issue_id, project_id, status, uri_segment, node_name, created_at, updated_at)
                     VALUES ('j-builder', 'e-1', 'i-1181', 'p-cairn', 'blocked', 'builder', 'builder', 1, 1)",
                    (),
                )
                .await?;
                if let Some(name) = artifact_name {
                    conn.execute(
                        "INSERT INTO artifacts (id, job_id, artifact_type, data, version, output_name, created_at, updated_at)
                         VALUES ('a-1', 'j-builder', 'pull_request', '{}', 1, ?1, 1, 1)",
                        params![name.as_str()],
                    )
                    .await?;
                }
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    async fn seed_external_reply(db: &LocalDb, marker: i64) {
        db.write(move |conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1', 'W', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p-cairn', 'w-1', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                     VALUES ('i-1181', 'p-cairn', 1181, 'T', '', 'active', 'in_progress', 'none', 0, 1, ?1)",
                    params![marker],
                )
                .await?;
                // The external reply's catch-up cursor is its own created_at.
                conn.execute(
                    "INSERT INTO messages (id, channel_type, channel_id, sender_run_id, sender_name, recipient_run_id, content, created_at)
                     VALUES ('m-external', 'issue', 'CAIRN/1181', 'run-builder', 'cairn://p/CAIRN/1181/1/builder', NULL, 'done', ?1)",
                    params![marker],
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn external_reply_catch_up_uses_message_marker_cursor() {
        let db = test_db().await;
        seed_external_reply(&db, 101).await;

        let event = latest_external_reply_event(&db, &test_issue_ref(), Some(100))
            .await
            .unwrap()
            .expect("external reply newer than cursor");
        assert_eq!(event.updated_at, 101);
        assert_eq!(event.attention, IssueAttention::None);
        assert!(matches!(
            event.fact,
            AttentionFact::ExternalMessageReply { .. }
        ));

        let none = latest_external_reply_event(&db, &test_issue_ref(), Some(101))
            .await
            .unwrap();
        assert!(none.is_none(), "strict cursor should suppress same marker");
    }

    #[tokio::test]
    async fn synthesize_event_uses_blocked_node_real_artifact_name() {
        let db = test_db().await;
        seed_blocked_node(&db, Some("pr")).await;
        assert_waiting_approval_detail_uri(&db, "cairn://p/CAIRN/1181/1/builder/pr").await;
    }

    #[tokio::test]
    async fn synthesize_event_falls_back_to_generic_artifact_alias() {
        let db = test_db().await;
        seed_blocked_node(&db, None).await;
        assert_waiting_approval_detail_uri(&db, "cairn://p/CAIRN/1181/1/builder/artifact").await;
    }

    /// Seed a project + issue + execution + a single blocked `pr` action_run
    /// (no blocked job). The detail URI must be the bare pr node, with no
    /// artifact name (CAIRN-1222).
    async fn seed_blocked_action_run(db: &LocalDb) {
        db.write(move |conn| {
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO workspaces (id, name, created_at, updated_at) VALUES ('w-1', 'W', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
                     VALUES ('p-cairn', 'w-1', 'Cairn', 'CAIRN', '/tmp/cairn', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO issues (id, project_id, number, title, description, status, progress, attention, priority, created_at, updated_at)
                     VALUES ('i-1181', 'p-cairn', 1181, 'T', '', 'waiting', 'waiting', 'needs_approval', 0, 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
                     VALUES ('e-1', 'default', 'i-1181', 'p-cairn', 'running', 1, 1)",
                    (),
                )
                .await?;
                conn.execute(
                    "INSERT INTO action_runs (id, execution_id, recipe_node_id, action_config_id, issue_id, project_id, status, created_at, uri_segment)
                     VALUES ('ar-pr', 'e-1', 'pr-1', 'builtin:pr', 'i-1181', 'p-cairn', 'blocked', 1, 'pr')",
                    (),
                )
                .await?;
                Ok::<_, DbError>(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn synthesize_event_uses_blocked_action_run_bare_node_uri() {
        let db = test_db().await;
        seed_blocked_action_run(&db).await;
        assert_waiting_approval_detail_uri(&db, "cairn://p/CAIRN/1181/1/pr").await;
    }

    #[test]
    fn evaluate_routes_actionable_and_not_yet() {
        assert!(matches!(
            evaluate(IssueAttention::NeedsInput, IssueStatus::Active, 100, None),
            WatchOutcome::Actionable
        ));
        assert!(matches!(
            evaluate(IssueAttention::None, IssueStatus::Active, 100, None),
            WatchOutcome::NotYet
        ));
        assert!(matches!(
            evaluate(
                IssueAttention::NeedsInput,
                IssueStatus::Active,
                100,
                Some(100)
            ),
            WatchOutcome::NotYet
        ));
    }
}
