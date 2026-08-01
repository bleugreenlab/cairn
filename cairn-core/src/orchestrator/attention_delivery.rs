//! Attention delivery engine (push-queue model, docs/attention-redesign.md).
//!
//! Creates and renders attention pushes. Two responsibilities:
//!
//! 1. **Create pushes.** [`create_resolved_push`] turns a terminal `Resolved`
//!    fact into a `resolved:{issue}` push to the issue's watchers. Failure is
//!    rousing; successful resolution remains passive.
//!    [`create_catchup_push`] creates the passive `catchup:{child-job}` push at
//!    the user→child message moment, resolved at delivery against the parent's
//!    read cursor. Question and permission pushes are created at their own emit
//!    sites (where the producing node is known) through
//!    [`push_to_issue_watchers`].
//! 2. **Render pushes at resume time.** [`render_pushes_resolved`] resolves most
//!    drained push `content_ref`s to rendered resource content so a resumed agent
//!    acts without a round-trip read. Terminal `resolved:` pushes are rendered as
//!    concise confirmations instead of dumping the resolved issue body.

use cairn_db::turso::params;

use super::attention_push::{Boundary, Wake};
use super::Orchestrator;
use crate::orchestrator::{AttentionEvent, AttentionFact};
use crate::storage::{run_db_blocking, LocalDb, RowExt};

/// Create the normalized `resolved:{issue}` push at a terminal-resolution emit.
///
/// The single creator of the resolved push, fed by every
/// `AttentionFact::Resolved` emit (the recompute terminal sweep, the work-turn
/// idle edge, the PR webhook, and `wake_for_issue`) through the
/// `emit_attention_event` funnel. Non-`Resolved` facts are ignored — question
/// and permission pushes are created at their own emit sites where the producing
/// node is known. Failed resolution wakes subscribed idle watchers; merged and
/// closed resolution remain passive ride-along information. Supersede-by-key
/// collapses repeat undelivered emits, while a status/update fingerprint prevents
/// the same terminal resolution from re-firing after delivery. The resolved child
/// issue's own jobs are excluded so a child never receives its own notification.
pub(crate) fn create_resolved_push(orch: &Orchestrator, event: &AttentionEvent) {
    let final_status = match &event.fact {
        AttentionFact::Resolved { final_status } => final_status.clone(),
        _ => return,
    };
    let wake = if final_status == crate::models::IssueStatus::Failed {
        Wake::Wake
    } else {
        Wake::Passive
    };
    let fingerprint = format!("status:{final_status}:{}", event.updated_at);
    let dbs = orch.db.clone();
    let issue_id = event.issue_id.clone();
    let issue_uri = event.issue_uri.clone();
    let result = run_db_blocking(move || async move {
        let db = crate::issues::crud::owning_db_for_issue(&dbs, &issue_id)
            .await
            .map_err(|e| e.to_string())?;
        let key = format!("resolved:{issue_uri}");
        let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(&db, &issue_uri).await?;
        let mut pushed = Vec::new();
        for recipient in watchers {
            if job_belongs_to_issue(&db, &recipient, &issue_id).await? {
                continue;
            }
            if let Some(Some(previous)) =
                super::attention_push::latest_push_fingerprint(&db, &recipient, &key)
                    .await
                    .map_err(|e| e.to_string())?
            {
                if previous == fingerprint {
                    continue;
                }
            }
            let (_, effective) = super::attention_push::push_with_fingerprint(
                &db,
                &recipient,
                &issue_uri,
                wake,
                Boundary::Event,
                &key,
                Some(&fingerprint),
            )
            .await
            .map_err(|e| e.to_string())?;
            if effective.wakes_idle() {
                pushed.push(recipient);
            }
        }
        Ok::<_, String>(pushed)
    });
    match result {
        Ok(recipients) => {
            orch.notifier.emit_change("attention_pushes");
            for recipient in recipients {
                if let Err(e) = crate::messages::delivery::nudge_job_for_urgency(
                    orch,
                    &recipient,
                    crate::messages::queued::DeliveryUrgency::Steer,
                ) {
                    log::warn!(
                        "resolved push wake for {} failed: {}",
                        &recipient[..recipient.len().min(8)],
                        e
                    );
                }
            }
        }
        Err(e) => log::warn!("resolved push creation failed: {}", e),
    }
}

async fn job_belongs_to_issue(db: &LocalDb, job_id: &str, issue_id: &str) -> Result<bool, String> {
    let job_id = job_id.to_string();
    let issue_id = issue_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        let issue_id = issue_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT 1 FROM jobs WHERE id=?1 AND issue_id=?2 LIMIT 1",
                    params![job_id.as_str(), issue_id.as_str()],
                )
                .await?;
            Ok(rows.next().await?.is_some())
        })
    })
    .await
    .map_err(|e| e.to_string())
}

/// Push to every watcher of `issue_uri`, optionally excluding the producing
/// node. The shared creator for question and permission push sources; review and
/// resolved pushes each have outcome-specific creators.
/// Supersede-by-key collapses repeats to one undelivered row per recipient; the
/// delivery layer drains, lazy-resolves, and stamps each push.
///
/// Returns the recipients that received a push, so a `wake`/`interrupt` caller
/// can wake each through `delivery::nudge_job_for_urgency` (CAIRN-1889). Passive
/// callers ignore the list.
pub(crate) async fn push_to_issue_watchers(
    db: &LocalDb,
    issue_uri: &str,
    exclude_job: Option<&str>,
    content_ref: &str,
    wake: Wake,
    boundary: Boundary,
    key: &str,
) -> Result<Vec<String>, String> {
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(db, issue_uri).await?;
    let mut pushed = Vec::new();
    for recipient in watchers {
        if Some(recipient.as_str()) == exclude_job {
            continue;
        }
        let (_, effective) =
            super::attention_push::push(db, &recipient, content_ref, wake, boundary, key)
                .await
                .map_err(|e| e.to_string())?;
        // Only hand rousing recipients back for nudging. A recipient that muted
        // this source gets a `Passive` ride-along row (created by the central
        // downgrade in `push`) and must NOT be woken (CAIRN-1900).
        if effective.wakes_idle() {
            pushed.push(recipient);
        }
    }
    Ok(pushed)
}

/// Create the passive `catchup:{child-job}` push for the watching parent at the
/// user→child message moment (CAIRN-1894). Single trigger, definite recipient
/// (the watching parent), definite source (the addressed child node or sub-task's
/// chat).
///
/// The cursor is keyed by the child JOB id whose `{node|task}/chat` this renders,
/// so it counts exactly the transcript the parent is shown (one job's runs)
/// rather than the whole issue's sibling jobs and sub-task runs. The window's
/// `content_ref` is `{child_uri}/chat?offset={start}` with no end bound:
/// [`render_push_resolved`] reads it AT DELIVERY, so the rendered window spans
/// from `start` through whatever that job has accrued by the time the parent next
/// runs — turns in that gap are included for free, with no turn-end bump. `start`
/// is the parent's read cursor when it has looked before, else one turn of
/// lead-in. Because the cursor only advances on delivery, a second message before
/// delivery reuses the same start. Passive: it never wakes the idle parent; it
/// rides along on the parent's next run.
pub(crate) async fn create_catchup_push(
    db: &LocalDb,
    parent_job_id: &str,
    child_uri: &str,
) -> Result<(), String> {
    let Some(child_job_id) = job_id_for_child_uri(db, child_uri).await else {
        // The URI did not resolve to an agent job (a stale or not-yet-persisted
        // node/task, or a non-node/task URI); skip rather than mis-scope the
        // cursor.
        log::warn!("catch-up push: no job for {child_uri}");
        return Ok(());
    };
    let tail = count_job_chat_turns(db, &child_job_id).await;
    let start = match super::attention_push::read_cursor(db, parent_job_id, &child_job_id)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(cursor) => cursor,
        None => (tail - 1).max(0),
    };
    let content_ref = format!("{child_uri}/chat?offset={start}");
    let key = format!("catchup:{child_job_id}");
    super::attention_push::push(
        db,
        parent_job_id,
        &content_ref,
        Wake::Passive,
        Boundary::Event,
        &key,
    )
    .await
    .map_err(|e| e.to_string())?;
    Ok(())
}

/// Create the passive catch-up push for **every job that watches** the issue the
/// addressed node belongs to (CAIRN-3342). The fan-out an operator message goes
/// through; [`create_catchup_push`] is the per-recipient primitive underneath it.
///
/// The recipient set is derived, never materialized: [`watcher_jobs_for_issue`]
/// resolves the issue's current coordinator on every call, plus any explicit
/// subscription rows. A new coordinator execution therefore inherits its
/// children's operator traffic with no hand-off step, and a superseded one stops
/// receiving it. The message path used to resolve its single recipient through
/// `parent_wake::load_parent_job`, which prefers the `issues.parent_job_id`
/// spawning snapshot — the same cache CAIRN-3293 removed from every other
/// child-attention route, and which in practice addressed catch-up to a
/// coordinator that had already been superseded.
///
/// Jobs on the addressed node's own issue are excluded, so a node never receives
/// catch-up for its own chat. Mute composes rather than suppresses: the row is
/// created `Passive`, which is exactly what a mute would downgrade a wake to, so
/// a muted child's operator traffic still rides along on the watcher's next run.
///
/// [`watcher_jobs_for_issue`]: crate::orchestrator::wakes::watcher_jobs_for_issue
pub(crate) async fn create_catchup_pushes_for_watchers(
    db: &LocalDb,
    child_uri: &str,
    exclude_job: Option<&str>,
) -> Result<usize, String> {
    let Some(parsed) = cairn_common::uri::parse_uri(child_uri) else {
        return Ok(0);
    };
    let (Some(project_key), Some(number)) = (
        parsed.project().map(str::to_uppercase),
        parsed.issue_number(),
    ) else {
        return Ok(0);
    };
    let issue_uri = cairn_common::uri::build_issue_uri(&project_key, number);
    let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(db, &issue_uri).await?;
    if watchers.is_empty() {
        return Ok(0);
    }
    let issue_id = issue_id_for_issue_uri(db, &issue_uri).await?;
    let mut created = 0usize;
    for recipient in watchers {
        if Some(recipient.as_str()) == exclude_job {
            continue;
        }
        if let Some(issue_id) = issue_id.as_deref() {
            if job_belongs_to_issue(db, &recipient, issue_id).await? {
                continue;
            }
        }
        create_catchup_push(db, &recipient, child_uri).await?;
        created += 1;
    }
    Ok(created)
}

/// The node/task URI of `job_id`, then [`create_catchup_pushes_for_watchers`] on
/// it. The entry point for callers holding a job rather than a URI — the desktop
/// composer, whose text reaches a job id and never an addressed URI.
pub(crate) async fn create_catchup_pushes_for_job(
    db: &LocalDb,
    child_job_id: &str,
) -> Result<usize, String> {
    let Some(child_uri) = crate::jobs::queries::home_uri_for_job(db, child_job_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    create_catchup_pushes_for_watchers(db, &child_uri, None).await
}

async fn issue_id_for_issue_uri(db: &LocalDb, issue_uri: &str) -> Result<Option<String>, String> {
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            Ok(
                crate::issues::relations::resolve_issue_uri(conn, &issue_uri)
                    .await?
                    .map(|issue| issue.issue_id),
            )
        })
    })
    .await
    .map_err(|error| error.to_string())
}

/// Resolve the agent job whose `{node|task}/chat` a catch-up push renders, from
/// the addressed child URI — the same job the chat resource resolves. A node URI
/// maps to its top-level job (issue + execution seq + node `uri_segment`); a task
/// URI maps to the addressed sub-task job (the task `uri_segment` under that node
/// job), so a user message directed at a sub-task still scopes catch-up to the
/// task's own chat. `None` for any other URI or an unresolved node/task.
async fn job_id_for_child_uri(db: &LocalDb, child_uri: &str) -> Option<String> {
    let (project, number, exec_seq, node_id, task_name) =
        match cairn_common::uri::parse_uri(child_uri)? {
            cairn_common::uri::CairnResource::Node {
                project,
                number,
                exec_seq,
                node_id,
            } => (project, number, exec_seq, node_id, None),
            cairn_common::uri::CairnResource::Task {
                project,
                number,
                exec_seq,
                node_id,
                task_name,
            } => (project, number, exec_seq, node_id, Some(task_name)),
            _ => return None,
        };
    let project = project.to_uppercase();
    let number = number as i64;
    let exec_seq = exec_seq as i64;
    db.read(|conn| {
        let project = project.clone();
        let node_id = node_id.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            // The top-level node job (the rendered job for a node URI, or the
            // task's parent for a task URI).
            let mut rows = conn
                .query(
                    "SELECT j.id FROM jobs j
                     JOIN executions e ON j.execution_id = e.id
                     JOIN issues i ON j.issue_id = i.id
                     JOIN projects p ON i.project_id = p.id
                     WHERE p.key=?1 AND i.number=?2 AND e.seq=?3
                       AND j.uri_segment=?4 AND j.parent_job_id IS NULL
                     LIMIT 1",
                    params![project.as_str(), number, exec_seq, node_id.as_str()],
                )
                .await?;
            let node_job_id = match rows.next().await? {
                Some(row) => row.text(0)?,
                None => return Ok::<Option<String>, crate::storage::DbError>(None),
            };
            // A node URI renders the node job itself; a task URI renders the
            // addressed sub-task job under it.
            let Some(task_name) = task_name else {
                return Ok(Some(node_job_id));
            };
            let mut task_rows = conn
                .query(
                    "SELECT id FROM jobs WHERE parent_job_id=?1 AND uri_segment=?2 LIMIT 1",
                    params![node_job_id.as_str(), task_name.as_str()],
                )
                .await?;
            match task_rows.next().await? {
                Some(row) => Ok(Some(row.text(0)?)),
                None => Ok(None),
            }
        })
    })
    .await
    .ok()
    .flatten()
}

/// Distinct turns recorded across one job's runs — the job-scoped chat tail that
/// `{node}/chat` renders. Catch-up's window start when the parent has no prior
/// read cursor.
async fn count_job_chat_turns(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COUNT(DISTINCT e.turn_id) FROM events e
                     JOIN runs r ON e.run_id = r.id
                     WHERE r.job_id = ?1 AND e.turn_id IS NOT NULL",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => Ok(row.i64(0)?),
                None => Ok(0),
            }
        })
    })
    .await
    .unwrap_or(0)
}

/// Resolve a single `cairn://` (or file/web) URI to rendered markdown via the
/// same `read_batch` path that backs `cairn read [uri]`. `run_id` is `None` so
/// the briefing never pollutes the agent's read-dedup state. Returns `None` on a
/// resolution failure (e.g. a fence suspension string, which is not an
/// envelope) or an empty body, so the caller can fall back to a bare pointer.
async fn resolve_uri_to_markdown(orch: &Orchestrator, uri: &str) -> Option<String> {
    let request = crate::mcp::types::McpCallbackRequest {
        thread_id: None,
        cwd: String::new(),
        run_id: None,
        tool: "read_batch".to_string(),
        payload: serde_json::json!({ "paths": [uri] }),
        tool_use_id: None,
    };
    let cursors = std::sync::Mutex::new(std::collections::HashMap::new());
    let raw = crate::mcp::handlers::read::handle_read_batch(orch, &request, &cursors).await;
    let envelope: cairn_common::read::ReadBatchEnvelope = serde_json::from_str(&raw).ok()?;
    let text = envelope.text.trim();
    if text.is_empty() {
        None
    } else {
        Some(text.to_string())
    }
}

/// Max characters of resolved push content inlined into a reminder/prompt
/// (CAIRN-1891). A full PR diff can be huge; cap to a useful rendered view and
/// keep the `content_ref` URI so the agent can follow it for the rest.
const PUSH_CONTENT_CAP: usize = 4000;

/// How many operator messages a catch-up digest names, and how much of each.
const CATCHUP_DIGEST_MESSAGES: usize = 5;
const CATCHUP_DIGEST_CHARS: usize = 240;

/// The operator messages a watcher has not caught up on for `child_job_id`, as a
/// bounded digest line per message (CAIRN-3342).
///
/// This exists because the catch-up window alone cannot carry the fact reliably.
/// The window is `{child}/chat?offset={cursor}` and [`cap_push_content`] cuts it
/// **from the front**, so for a busy child — whose unread window can run to
/// dozens of turns — the newest thing in it, which is precisely the operator's
/// message, is the first thing discarded. The digest is rendered ahead of the
/// window so the load-bearing fact survives the cap, and the window follows as
/// context.
///
/// The digest reads **two** durable sources, because an operator message is not
/// in the child's transcript the moment it is sent. Text typed at a busy child is
/// a `queued_messages` row until the child reaches a tool boundary or turn end and
/// claims it — which can be a whole builder turn later. A coordinator that runs
/// inside that gap would otherwise render an empty digest over a stale window,
/// stamp the push delivered, advance its read cursor past the child's current
/// tail, and retire the row; nothing re-fires when the child finally records the
/// message, so that operator intervention would be lost for good. Reading the
/// pending queue alongside the transcript makes the digest independent of that
/// ordering, and tells the coordinator sooner besides: it learns the operator
/// changed the child's scope before the child has even read it.
///
/// Both sources obey one rule — *operator text for this job that the recipient
/// has not already caught up on* — anchored on its read cursor `updated_at`,
/// which is stamped in the same transaction that delivered its last catch-up. For
/// the transcript that means `user` events at or after the anchor. For the queue
/// it means rows still pending **or claimed at or after the anchor**, because the
/// claim is not the moment the text appears in the transcript: `claim_for_job_inner`
/// commits the `delivered_at` stamp and its caller inserts the transcript event
/// afterwards, so a row selected only on `delivered_at IS NULL` would fall out of
/// both sources for the width of that gap. Keeping recently claimed rows closes
/// it; the content dedupe collapses the overlap once the event lands, keeping the
/// transcript copy since it is the one the node has actually read.
///
/// With no cursor yet (a first catch-up), the newest [`CATCHUP_DIGEST_MESSAGES`]
/// stand in — an unanchored scan would otherwise open with the issue body the
/// coordinator wrote itself. Both sources are read at delivery, so an edited or
/// deleted queued message reports its current state rather than a stale copy.
async fn catchup_operator_digest(
    db: &LocalDb,
    recipient: &str,
    child_job_id: &str,
) -> Option<String> {
    let anchor = super::attention_push::read_cursor_updated_at(db, recipient, child_job_id)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    let child_job_id = child_job_id.to_string();
    let (recorded, queued): (Vec<OperatorMessage>, Vec<OperatorMessage>) = db
        .read(|conn| {
            let child_job_id = child_job_id.clone();
            Box::pin(async move {
                let limit = CATCHUP_DIGEST_MESSAGES as i64;
                let mut recorded = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT e.timestamp, e.data FROM events e
                         JOIN runs r ON r.id = e.run_id
                         WHERE r.job_id = ?1 AND e.event_type = 'user' AND e.timestamp >= ?2
                         ORDER BY e.timestamp DESC, e.sequence DESC
                         LIMIT ?3",
                        params![child_job_id.as_str(), anchor, limit],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    // An archived event stores its body out of line; there is
                    // nothing to excerpt, so leave it to the chat window.
                    if let Some(content) = user_event_content(&row.text(1)?) {
                        recorded.push(OperatorMessage {
                            timestamp: row.i64(0)?,
                            content,
                            read_by_node: true,
                        });
                    }
                }
                let mut queued = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT created_at, content, delivered_at FROM queued_messages
                         WHERE job_id = ?1
                           AND (delivered_at IS NULL OR delivered_at >= ?2)
                         ORDER BY created_at DESC
                         LIMIT ?3",
                        params![child_job_id.as_str(), anchor, limit],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    let content = row.text(1)?;
                    if !content.trim().is_empty() {
                        queued.push(OperatorMessage {
                            timestamp: row.i64(0)?,
                            content,
                            read_by_node: row.opt_i64(2)?.is_some(),
                        });
                    }
                }
                Ok((recorded, queued))
            })
        })
        .await
        .ok()?;

    let mut messages = reconcile_operator_messages(recorded, queued);
    // Newest first, so the cap keeps the newest.
    messages.sort_by_key(|message| std::cmp::Reverse(message.timestamp));
    messages.truncate(CATCHUP_DIGEST_MESSAGES);
    if messages.is_empty() {
        return None;
    }
    let count = messages.len();
    let noun = if count == 1 { "message" } else { "messages" };
    let lines: Vec<String> = messages
        .iter()
        .rev()
        .map(|message| {
            let when = chrono::DateTime::from_timestamp(message.timestamp, 0)
                .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
                .unwrap_or_else(|| "unknown time".to_string());
            let state = if message.read_by_node {
                String::new()
            } else {
                " (queued, not yet read by the node)".to_string()
            };
            format!("  • {when}{state} — {}", one_line_excerpt(&message.content))
        })
        .collect();
    Some(format!(
        "The operator sent {count} {noun} directly to this node that you have not caught up on:\n{}",
        lines.join("\n")
    ))
}

/// Combine the two sources into one message per operator send.
///
/// A message in flight is represented twice — once as the queue row it was typed
/// into and once as the transcript event the node recorded — and the digest must
/// show it once. Matching is by content, since the two representations share no
/// identifier, but it is a **multiset** reconciliation rather than content
/// uniqueness: the operator genuinely does repeat short messages (`^`, `looks
/// good!`, `please stop`), and collapsing those to one line would hide the
/// emphasis that made them worth repeating. Each recorded copy therefore cancels
/// at most one queue row, and only a **claimed** row is eligible — a row the node
/// has not yet read cannot already be in its transcript, so it always stands on
/// its own.
fn reconcile_operator_messages(
    recorded: Vec<OperatorMessage>,
    queued: Vec<OperatorMessage>,
) -> Vec<OperatorMessage> {
    let mut unmatched: std::collections::HashMap<&str, usize> = std::collections::HashMap::new();
    for message in &recorded {
        *unmatched.entry(message.content.as_str()).or_default() += 1;
    }
    let mut messages: Vec<OperatorMessage> = queued
        .iter()
        .filter(|message| {
            if !message.read_by_node {
                return true;
            }
            match unmatched.get_mut(message.content.as_str()) {
                Some(remaining) if *remaining > 0 => {
                    *remaining -= 1;
                    false
                }
                _ => true,
            }
        })
        .cloned()
        .collect();
    messages.extend(recorded);
    messages
}

/// One operator message bound for a child node, from either durable source.
#[derive(Clone)]
struct OperatorMessage {
    timestamp: i64,
    content: String,
    /// `false` while the text is still a pending `queued_messages` row the child
    /// has not claimed — sent by the operator, not yet seen by the node.
    read_by_node: bool,
}

/// The text of a `user` transcript event, or `None` when it carries none.
fn user_event_content(data: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(data)
        .ok()?
        .get("content")?
        .as_str()
        .map(str::to_string)
        .filter(|content| !content.trim().is_empty())
}

/// Collapse a message to one line of at most [`CATCHUP_DIGEST_CHARS`].
fn one_line_excerpt(content: &str) -> String {
    let flattened = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if flattened.chars().count() <= CATCHUP_DIGEST_CHARS {
        return flattened;
    }
    let truncated: String = flattened.chars().take(CATCHUP_DIGEST_CHARS).collect();
    format!("{truncated}…")
}

async fn resolved_issue_confirmation(orch: &Orchestrator, issue_uri: &str) -> Option<String> {
    let project_key = cairn_common::uri::parse_uri(issue_uri)?
        .project_key()?
        .to_string();
    let db = orch.db.for_project(&project_key).await;
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            let Some(issue) = crate::issues::relations::resolve_issue_uri(conn, &issue_uri).await?
            else {
                return Ok(None);
            };
            let message = match issue.status {
                crate::models::IssueStatus::Merged => "Issue Merged Successfully",
                crate::models::IssueStatus::Closed => "Issue Closed Successfully",
                crate::models::IssueStatus::Failed => {
                    "Issue Failed — inspect the child and retry or delegate a fix"
                }
                _ => return Ok(None),
            };
            Ok(Some(message.to_string()))
        })
    })
    .await
    .ok()
    .flatten()
}

/// Render a drained attention push with its referent content resolved inline
/// (CAIRN-1891), so the agent acts without a round-trip read. The header carries
/// the wake level and the `content_ref` URI; the body is the rendered resource
/// (the PR summary/diff, plan, question, or permission), capped. Resolution uses
/// the same in-process read that backs `cairn read {uri}` (and the briefing).
/// Terminal `resolved:` pushes are intentionally concise confirmations instead
/// of a full issue read. Falls back to the bare header line when resolution
/// yields nothing, so the agent still has the URI to follow.
pub(crate) async fn render_push_resolved(
    orch: &Orchestrator,
    push: &crate::orchestrator::attention_push::Push,
) -> String {
    let header = format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    );
    if push.key.starts_with("resolved:") {
        return match resolved_issue_confirmation(orch, &push.content_ref).await {
            Some(body) => format!("{header}\n\n{body}"),
            None => header,
        };
    }

    // A `direct:` push carries frozen message content, not an idempotent
    // resolvable referent. Resolve it from the durable `messages` row by the
    // message id in the key (`direct:{message_id}`) rather than from
    // `content_ref` (which is the conversation surface the wake card links to).
    // Falls back to the header line if the row is missing (CAIRN-1900).
    if let Some(message_id) = push.key.strip_prefix("direct:") {
        return match crate::messages::db::get_message_by_id_async(&orch.db.local, message_id).await
        {
            Ok(Some(msg)) => {
                let body = crate::messages::render::render_direct_message(&msg);
                format!("{header}\n\n{body}")
            }
            _ => header,
        };
    }
    // A `catchup:` push leads with the operator-message digest and follows with
    // the chat window, so the fact the coordinator must not miss sits ahead of
    // the cap rather than behind it (CAIRN-3342).
    let digest = match push.key.strip_prefix("catchup:") {
        Some(child_job_id) => {
            let db = crate::execution::routing::owning_db_for_job(&orch.db, child_job_id)
                .await
                .unwrap_or_else(|_| orch.db.local.clone());
            catchup_operator_digest(&db, &push.recipient, child_job_id).await
        }
        None => None,
    };
    let body = resolve_uri_to_markdown(orch, &push.content_ref)
        .await
        .map(|body| cap_push_content(&body, &push.content_ref));
    match (digest, body) {
        (Some(digest), Some(body)) => format!("{header}\n\n{digest}\n\n{body}"),
        (Some(digest), None) => format!("{header}\n\n{digest}"),
        (None, Some(body)) => format!("{header}\n\n{body}"),
        (None, None) => header,
    }
}

/// Resolve and render several pushes into one block (CAIRN-1891), or `None` when
/// the slice is empty so callers can fold it into an optional prompt section.
pub(crate) async fn render_pushes_resolved(
    orch: &Orchestrator,
    pushes: &[crate::orchestrator::attention_push::Push],
) -> Option<String> {
    if pushes.is_empty() {
        return None;
    }
    let mut blocks = Vec::with_capacity(pushes.len());
    for push in pushes {
        blocks.push(render_push_resolved(orch, push).await);
    }
    Some(blocks.join("\n\n"))
}

/// Cap resolved push content to [`PUSH_CONTENT_CAP`] characters on a char
/// boundary, appending a pointer to the full resource when truncated.
fn cap_push_content(body: &str, uri: &str) -> String {
    if body.chars().count() <= PUSH_CONTENT_CAP {
        return body.to_string();
    }
    let truncated: String = body.chars().take(PUSH_CONTENT_CAP).collect();
    format!("{truncated}\n\n… [truncated — read {uri} for the full content]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    const CHILD_URI: &str = "cairn://p/PROJ/2";

    #[test]
    fn cap_push_content_passes_short_content_through() {
        let body = "a short rendered body";
        assert_eq!(cap_push_content(body, "cairn://p/PROJ/2"), body);
    }

    #[test]
    fn cap_push_content_truncates_and_points_to_uri() {
        let body = "x".repeat(PUSH_CONTENT_CAP + 500);
        let uri = "cairn://p/PROJ/2/1/builder/pr";
        let capped = cap_push_content(&body, uri);
        assert!(capped.chars().count() < body.chars().count());
        assert!(capped.contains("truncated"));
        assert!(
            capped.contains(uri),
            "truncation must keep a pointer to the full resource"
        );
    }

    #[test]
    fn cap_push_content_respects_char_boundaries() {
        // A multi-byte char at the cap boundary must not panic the truncation.
        let body = "\u{1f600}".repeat(PUSH_CONTENT_CAP + 10);
        let capped = cap_push_content(&body, "cairn://p/PROJ/2");
        assert!(capped.contains("truncated"));
    }

    async fn migrated_db() -> LocalDb {
        crate::storage::migrated_test_db("attention-delivery.db").await
    }

    /// Parent issue + watcher job, child issue-1 with a child job + run, and a
    /// watcher subscription to the child issue.
    async fn seed(
        db: &LocalDb,
        sub_state: &str,
        fact_kinds_json: Option<&str>,
        until_kind: Option<&str>,
    ) {
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','PROJ','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('parent','p',1,'Parent','active','active','none',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none',1,1);
            INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
              VALUES('watcher','p','parent','running','sess',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec-1','r','issue-1','p','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('child-job','p','issue-1','exec-1','builder','running','sess2',1,1);
            INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
              VALUES('run-1','p','child-job','issue-1',1,1);
            ",
        )
        .await
        .unwrap();
        let sub_state = sub_state.to_string();
        let fact_kinds_json = fact_kinds_json.map(str::to_string);
        let until_kind = until_kind.map(str::to_string);
        db.write(move |conn| {
            let sub_state = sub_state.clone();
            let fact_kinds_json = fact_kinds_json.clone();
            let until_kind = until_kind.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                       (id, job_id, source_kind, source_ref, fact_kinds_json, state,
                        mute_until_kind, mute_until_ref, created_by, created_at, updated_at, one_shot)
                     VALUES('sub-1','watcher','issue',?1,?2,?3,?4,NULL,'agent',1,1,0)",
                    params![CHILD_URI, fact_kinds_json.as_deref(), sub_state.as_str(), until_kind.as_deref()],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    // ---- Push creators (CAIRN-1887) -----------------------------------------

    fn test_orchestrator(db: LocalDb) -> Orchestrator {
        use crate::db::DbState;
        use crate::orchestrator::OrchestratorBuilder;
        use crate::services::testing::TestServicesBuilder;
        use crate::storage::SearchIndex;
        use std::sync::Arc;
        let temp = tempfile::tempdir().unwrap();
        let root = temp.keep();
        let config_dir = root.join("config");
        std::fs::create_dir_all(config_dir.join("agents")).unwrap();
        std::fs::create_dir_all(config_dir.join("recipes")).unwrap();
        let search_index = Arc::new(SearchIndex::open_or_create(root.join("search")).unwrap());
        let db_state = Arc::new(DbState::new(Arc::new(db), search_index));
        let services = Arc::new(TestServicesBuilder::new().build());
        OrchestratorBuilder::new(db_state, services, config_dir).build()
    }

    #[tokio::test]
    async fn render_resolved_push_uses_concise_confirmation_not_issue_body() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "UPDATE issues
             SET status='merged', description='This long child issue description should not be inlined.'
             WHERE id='issue-1';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let push = crate::orchestrator::attention_push::Push {
            id: "push-1".into(),
            recipient: "watcher".into(),
            content_ref: CHILD_URI.into(),
            wake: Wake::Passive,
            boundary: Boundary::Event,
            key: format!("resolved:{CHILD_URI}"),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(rendered.contains("Attention update (passive): cairn://p/PROJ/2"));
        assert!(rendered.contains("Issue Merged Successfully"));
        assert!(!rendered.contains("Description"));
        assert!(!rendered.contains("This long child issue description"));
    }

    #[tokio::test]
    async fn render_failed_resolution_routes_to_team_replica() {
        let local = migrated_db().await;
        let team = std::sync::Arc::new(migrated_db().await);
        team.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('team-project','default','Team Project','TEAM','/tmp/team',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('team-issue','team-project',9,'Failed child','failed','failed','none',1,2);
            ",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(local);
        orch.db
            .register_team_db_for_test("team-1".to_string(), team)
            .await;
        orch.db.set_route("TEAM", Some("team-1".to_string())).await;
        let issue_uri = "cairn://p/TEAM/9";
        let push = crate::orchestrator::attention_push::Push {
            id: "team-push".into(),
            recipient: "coordinator".into(),
            content_ref: issue_uri.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: format!("resolved:{issue_uri}"),
            created_at: 2,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(rendered.contains("Issue Failed"), "{rendered}");
        assert!(rendered.contains("retry or delegate a fix"), "{rendered}");
        assert!(!rendered.contains("Successfully"), "{rendered}");
    }

    #[tokio::test]
    async fn render_unconfirmed_resolution_is_bare_header() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        let orch = test_orchestrator(db);

        for issue_uri in [CHILD_URI, "cairn://p/PROJ/999"] {
            let push = crate::orchestrator::attention_push::Push {
                id: format!("unconfirmed-{issue_uri}"),
                recipient: "coordinator".into(),
                content_ref: issue_uri.into(),
                wake: Wake::Wake,
                boundary: Boundary::Event,
                key: format!("resolved:{issue_uri}"),
                created_at: 2,
                delivered_event_id: None,
            };

            let rendered = render_push_resolved(&orch, &push).await;

            assert_eq!(rendered, format!("Attention update (wake): {issue_uri}"));
            assert!(!rendered.contains("Successfully"), "{rendered}");
        }
    }

    /// Subscribe `job_id` to the child issue (`CHILD_URI`).
    async fn add_issue_sub(db: &LocalDb, job_id: &str) {
        let job_id = job_id.to_string();
        db.write(move |conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO wake_subscriptions
                       (id, job_id, source_kind, source_ref, state, created_by, created_at, updated_at, one_shot)
                     VALUES(?1, ?2, 'issue', ?3, 'active', 'agent', 1, 1, 0)",
                    params![format!("sub-{job_id}"), job_id.as_str(), CHILD_URI],
                )
                .await?;
                Ok(())
            })
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn push_to_issue_watchers_excludes_producing_node() {
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        // 'watcher' is subscribed to the child issue; subscribe the producing
        // node 'child-job' to the same issue so the exclusion is exercised.
        seed(&db, "active", None, None).await;
        add_issue_sub(&db, "child-job").await;

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/PROJ/2/1/planner/questions/q-1",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/PROJ/2",
        )
        .await
        .unwrap();

        let watcher = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].boundary, Boundary::Event);
        assert_eq!(watcher[0].key, "question:cairn://p/PROJ/2");
        assert_eq!(
            watcher[0].content_ref,
            "cairn://p/PROJ/2/1/planner/questions/q-1"
        );
        // The producing node never receives its own push.
        assert!(list_pending(&db, "child-job").await.unwrap().is_empty());
    }

    /// The CAIRN-3293 specimen shape: a child issue is parented to the
    /// coordinator's issue and produces a gate, while no `wake_subscriptions` row
    /// exists anywhere. Before the recipient was derived, no push row was ever
    /// created and the gate waited until a human noticed.
    #[tokio::test]
    async fn a_gate_reaches_the_coordinator_with_no_subscription_row() {
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "DELETE FROM wake_subscriptions;
             UPDATE issues SET parent_issue_id = 'parent' WHERE id = 'issue-1';",
        )
        .await
        .unwrap();

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/PROJ/2/1/planner/plan",
            Wake::Wake,
            Boundary::Event,
            &format!("review:{CHILD_URI}"),
        )
        .await
        .unwrap();

        let watcher = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1, "the coordinating node receives the gate");
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].content_ref, "cairn://p/PROJ/2/1/planner/plan");
        // The child's own node is still excluded from its own gate.
        assert!(list_pending(&db, "child-job").await.unwrap().is_empty());
    }

    /// The existing-install shape behind migration `0130`: a superseded
    /// coordinator that still holds a seeded child-attention row. While that row
    /// exists it stays in the watcher set and the push creators write it a row for
    /// a child it no longer owns. `muted` is not a free pass — it only makes the
    /// row `passive`, which still rides along the next time that job runs. This is
    /// why the migration deletes `muted` seeded defaults and not just `active`
    /// ones; with the row gone, the derivation names the successor alone.
    #[tokio::test]
    async fn a_superseded_coordinators_seeded_row_is_what_keeps_it_watching() {
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // `watcher` is the retired coordinator holding a muted seeded default;
        // `successor` is the newer execution's coordinator on the same parent.
        db.execute_script(
            "DELETE FROM wake_subscriptions;
             UPDATE issues SET parent_issue_id = 'parent' WHERE id = 'issue-1';
             INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
               VALUES('successor','p','parent','running','sess3',5,5);
             INSERT INTO wake_subscriptions
               (id, job_id, source_kind, source_ref, fact_kinds_json, state, created_by, created_at, updated_at, one_shot)
               VALUES('seeded','watcher','issue','cairn://p/PROJ/2',
                      '[\"message\",\"permission\",\"question\",\"resolved\",\"review\"]','muted','system',1,1,0);",
        )
        .await
        .unwrap();
        let key = format!("review:{CHILD_URI}");

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/PROJ/2/1/planner/plan",
            Wake::Wake,
            Boundary::Event,
            &key,
        )
        .await
        .unwrap();

        // Pre-migration: the stale row draws a passive ride-along for a child the
        // retired coordinator no longer drives.
        let stale = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(stale.len(), 1, "the lingering row is what causes this");
        assert_eq!(stale[0].wake, Wake::Passive);
        assert_eq!(
            list_pending(&db, "successor").await.unwrap()[0].wake,
            Wake::Wake,
            "the live coordinator is roused either way"
        );

        // What migration 0130 does. Clear the pushes too, so what follows can only
        // come from a fresh creation.
        db.execute_script(
            "DELETE FROM wake_subscriptions WHERE id = 'seeded';
             DELETE FROM attention_pushes;",
        )
        .await
        .unwrap();

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/PROJ/2/1/planner/plan",
            Wake::Wake,
            Boundary::Event,
            &key,
        )
        .await
        .unwrap();

        assert!(
            list_pending(&db, "watcher").await.unwrap().is_empty(),
            "post-migration the retired coordinator receives no push at all"
        );
        let successor = list_pending(&db, "successor").await.unwrap();
        assert_eq!(successor.len(), 1);
        assert_eq!(successor[0].wake, Wake::Wake);
    }

    #[tokio::test]
    async fn create_resolved_push_keeps_successful_resolution_passive() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        for final_status in [IssueStatus::Merged, IssueStatus::Closed] {
            let db = migrated_db().await;
            seed(&db, "active", None, None).await;
            let orch = test_orchestrator(db);

            super::create_resolved_push(
                &orch,
                &AttentionEvent {
                    issue_id: "issue-1".into(),
                    issue_uri: CHILD_URI.into(),
                    fact: AttentionFact::Resolved {
                        final_status: final_status.clone(),
                    },
                    attention: IssueAttention::None,
                    status: final_status,
                    updated_at: 1,
                },
            );

            let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
            assert_eq!(watcher.len(), 1);
            // Successful resolution is informational: passive, rides along.
            assert_eq!(watcher[0].wake, Wake::Passive);
            assert_eq!(watcher[0].key, format!("resolved:{CHILD_URI}"));
            assert_eq!(watcher[0].content_ref, CHILD_URI);
        }
    }

    #[tokio::test]
    async fn create_resolved_push_wakes_watcher_for_failed_child() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Failed,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Failed,
                updated_at: 1,
            },
        );

        let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].key, format!("resolved:{CHILD_URI}"));
    }

    #[tokio::test]
    async fn muted_failed_resolution_is_passive() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "muted", Some(r#"["resolved"]"#), None).await;
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Failed,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Failed,
                updated_at: 1,
            },
        );

        let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Passive);
    }

    #[tokio::test]
    async fn resolved_child_does_not_push_to_its_own_subscribed_job() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_issue_sub(&db, "child-job").await;
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Failed,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Failed,
                updated_at: 1,
            },
        );

        assert!(list_pending(&orch.db.local, "child-job")
            .await
            .unwrap()
            .is_empty());
        assert_eq!(
            list_pending(&orch.db.local, "watcher").await.unwrap().len(),
            1
        );
    }

    // ---- Catch-up push creator (CAIRN-1894) ----------------------------------

    /// Insert a chat event carrying `turn_id` on the child issue's run so
    /// `child_chat_turn_count` sees a distinct turn.
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
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// Undelivered catch-up pushes for a recipient.
    async fn pending_catchup(
        db: &LocalDb,
        recipient: &str,
    ) -> Vec<crate::orchestrator::attention_push::Push> {
        crate::orchestrator::attention_push::list_pending(db, recipient)
            .await
            .unwrap()
            .into_iter()
            .filter(|p| p.key.starts_with("catchup:"))
            .collect()
    }

    /// Insert a carrying event, stamp the pushes delivered, and advance their
    /// read cursors in one transaction — the real delivery seam.
    async fn deliver(db: &LocalDb, push_ids: &[String], event_id: &str, seq: i64) {
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
                crate::orchestrator::attention_push::stamp_delivered_conn(
                    conn, &push_ids, &event_id,
                )
                .await?;
                crate::orchestrator::attention_push::advance_read_cursors_conn(conn, &push_ids)
                    .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    fn child_node_uri() -> String {
        format!("{CHILD_URI}/1/builder")
    }

    #[tokio::test]
    async fn child_turns_without_a_message_create_no_catchup() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The child does autonomous work — turns accrue — but no user→child
        // message fires. Catch-up is gated on the message moment, so none appears.
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        assert!(
            pending_catchup(&db, "watcher").await.is_empty(),
            "autonomous child turns must not generate catch-up"
        );
    }

    #[tokio::test]
    async fn user_child_message_creates_one_passive_catchup_push() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await; // tail = 1
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Passive);
        assert_eq!(pushes[0].key, "catchup:child-job");
        // No prior cursor -> one turn of lead-in (tail - 1 = 0).
        assert!(pushes[0].content_ref.ends_with("/chat?offset=0"));
        assert!(
            !crate::orchestrator::attention_push::has_pending_waking_live(&db, "watcher")
                .await
                .unwrap(),
            "a passive catch-up push never wakes an idle parent"
        );
    }

    #[tokio::test]
    async fn delivered_window_spans_to_current_tail_and_advances_cursor() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await; // message moment: T0 = 1
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        // The child works on before the parent resumes: T1 = 3.
        add_chat_turn(&db, "t2", 2).await;
        add_chat_turn(&db, "t3", 3).await;
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        // Window start frozen at creation (T0 - 1 = 0); the end is open and read
        // at delivery, so the gap turns are included for free.
        assert!(pushes[0].content_ref.ends_with("/chat?offset=0"));
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(3),
            "cursor advances to the child's tail at delivery"
        );
    }

    #[tokio::test]
    async fn second_message_after_delivery_opens_fresh_window_from_cursor() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        add_chat_turn(&db, "t3", 3).await; // T1 = 3
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let first = pending_catchup(&db, "watcher").await;
        deliver(&db, std::slice::from_ref(&first[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(3)
        );

        // More child turns, then a SECOND user→child message.
        add_chat_turn(&db, "t4", 4).await;
        add_chat_turn(&db, "t5", 5).await; // tail = 5
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let second = pending_catchup(&db, "watcher").await;
        assert_eq!(
            second.len(),
            1,
            "the delivered row left the queue; a fresh undelivered row opens"
        );
        assert!(
            second[0].content_ref.ends_with("/chat?offset=3"),
            "fresh window starts at the advanced cursor (3), not the new tail"
        );
    }

    #[tokio::test]
    async fn rolled_back_delivery_leaves_cursor_and_redelivers() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let id = pending_catchup(&db, "watcher").await[0].id.clone();

        // Deliver, then force the carrying transaction to roll back.
        let res = db
            .write(move |conn| {
                let id = id.clone();
                Box::pin(async move {
                    conn.execute(
                        "INSERT INTO events(id, run_id, sequence, timestamp, event_type, data, created_at)
                         VALUES('carry-x','run-1',100,1,'system:message','{}',1)",
                        (),
                    )
                    .await?;
                    crate::orchestrator::attention_push::stamp_delivered_conn(
                        conn,
                        std::slice::from_ref(&id),
                        "carry-x",
                    )
                    .await?;
                    crate::orchestrator::attention_push::advance_read_cursors_conn(
                        conn,
                        std::slice::from_ref(&id),
                    )
                    .await?;
                    Err::<(), crate::storage::DbError>(crate::storage::DbError::Row(
                        "forced rollback".into(),
                    ))
                })
            })
            .await;
        assert!(res.is_err());

        // Event, stamp, and cursor advance roll back together: catch-up redelivers
        // against the OLD (absent) cursor.
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            None
        );
        assert_eq!(pending_catchup(&db, "watcher").await.len(), 1);
    }

    #[tokio::test]
    async fn cursor_ignores_sibling_and_subtask_turns_on_the_same_issue() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The addressed child node's job (child-job) has 2 chat turns.
        add_chat_turn(&db, "t1", 1).await;
        add_chat_turn(&db, "t2", 2).await;
        // A sub-task job on the SAME issue accrues its own turns on its own run.
        // node chat (job-scoped) never shows these, so the cursor must not count
        // them — issue-scoped counting (the bug) would have reported 4.
        db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, parent_job_id, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES('task-job','p','issue-1','child-job','explore','running','sess3',1,1);
             INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES('run-task','p','task-job','issue-1',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk1','sess3','run-task',1,'completed',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk2','sess3','run-task',2,'completed',1,1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk1','run-task','tk1',1,1,'assistant','{}',1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk2','run-task','tk2',2,1,'assistant','{}',1);",
        )
        .await
        .unwrap();

        create_catchup_push(&db, "watcher", &child_node_uri())
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(2),
            "cursor counts only the addressed job's turns, not the issue's sub-task runs"
        );
    }

    // ---- Derived watchers + operator digest (CAIRN-3342) ---------------------

    /// Insert an operator message event on the child job's run.
    async fn add_user_message(db: &LocalDb, id: &str, seq: i64, timestamp: i64, content: &str) {
        let id = id.to_string();
        let data = serde_json::json!({ "eventType": "user", "content": content }).to_string();
        db.write(move |conn| {
            let id = id.clone();
            let data = data.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1','t1',?2,?3,'user',?4,1)",
                    params![id.as_str(), seq, timestamp, data.as_str()],
                )
                .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// Queue operator text at the child job without recording it in the
    /// transcript — the state a busy child sits in until it claims the queue.
    async fn queue_operator_message(db: &LocalDb, id: &str, created_at: i64, content: &str) {
        let id = id.to_string();
        let content = content.to_string();
        db.write(move |conn| {
            let id = id.clone();
            let content = content.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO queued_messages(id, job_id, content, delivery, created_at, delivered_at)
                     VALUES(?1,'child-job',?2,'queue',?3,NULL)",
                    params![id.as_str(), content.as_str(), created_at],
                )
                .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// `user` transcript events recorded across the child job's runs.
    async fn user_event_count(db: &LocalDb) -> i64 {
        db.query_one(
            "SELECT COUNT(*) FROM events e JOIN runs r ON r.id = e.run_id
             WHERE r.job_id = 'child-job' AND e.event_type = 'user'",
            (),
            |row| row.i64(0),
        )
        .await
        .unwrap()
    }

    /// The CAIRN-3342 specimen. The operator messages a child node whose
    /// coordinator has been superseded by a newer execution on the parent issue.
    /// `issues.parent_job_id` still names the retired spawner — exactly what the
    /// retired `load_parent_job` route would have addressed — while the derived
    /// recipient is the coordinator actually driving the parent now.
    #[tokio::test]
    async fn operator_message_catchup_reaches_the_live_coordinator_not_the_spawner() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "DELETE FROM wake_subscriptions;
             UPDATE issues SET parent_issue_id='parent', parent_job_id='watcher' WHERE id='issue-1';
             INSERT INTO jobs(id, project_id, issue_id, status, current_session_id, created_at, updated_at)
               VALUES('successor','p','parent','running','sess3',5,5);",
        )
        .await
        .unwrap();
        add_chat_turn(&db, "t1", 1).await;

        let created = create_catchup_pushes_for_watchers(&db, &child_node_uri(), None)
            .await
            .unwrap();

        assert_eq!(created, 1);
        let successor = pending_catchup(&db, "successor").await;
        assert_eq!(successor.len(), 1, "the live coordinator hears about it");
        assert_eq!(successor[0].wake, Wake::Passive);
        assert_eq!(successor[0].key, "catchup:child-job");
        assert!(
            pending_catchup(&db, "watcher").await.is_empty(),
            "the superseded spawner named by issues.parent_job_id is not a recipient"
        );
        assert!(
            !crate::orchestrator::attention_push::has_pending_waking_live(&db, "successor")
                .await
                .unwrap(),
            "an operator message to a child must never wake the coordinator"
        );
    }

    #[tokio::test]
    async fn a_node_never_receives_catchup_for_its_own_chat() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_issue_sub(&db, "child-job").await;
        add_chat_turn(&db, "t1", 1).await;

        create_catchup_pushes_for_watchers(&db, &child_node_uri(), None)
            .await
            .unwrap();

        assert_eq!(pending_catchup(&db, "watcher").await.len(), 1);
        assert!(pending_catchup(&db, "child-job").await.is_empty());
    }

    /// Mute suppresses wakes, not facts (CAIRN-3238). A catch-up row is `Passive`
    /// at creation — what a mute would downgrade a wake to anyway — so a muted
    /// watcher still gets the ride-along.
    #[tokio::test]
    async fn a_muted_watcher_still_gets_the_passive_catchup() {
        let db = migrated_db().await;
        seed(&db, "muted", None, None).await;
        add_chat_turn(&db, "t1", 1).await;

        create_catchup_pushes_for_watchers(&db, &child_node_uri(), None)
            .await
            .unwrap();

        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Passive);
    }

    #[tokio::test]
    async fn a_sender_is_not_told_about_its_own_message() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;

        let created = create_catchup_pushes_for_watchers(&db, &child_node_uri(), Some("watcher"))
            .await
            .unwrap();

        assert_eq!(created, 0);
        assert!(pending_catchup(&db, "watcher").await.is_empty());
    }

    #[tokio::test]
    async fn catchup_digest_names_operator_messages_since_the_read_cursor() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_user_message(&db, "u-old", 10, 100, "this one was already caught up on").await;
        add_user_message(&db, "u-new", 11, 300, "queue drained for days, good to go").await;
        db.execute(
            "INSERT INTO attention_read_cursors(recipient, source, position, updated_at)
             VALUES('watcher','child-job',1,200)",
            (),
        )
        .await
        .unwrap();

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .expect("an unread operator message produces a digest");

        assert!(digest.contains("1 message"), "{digest}");
        assert!(
            digest.contains("queue drained for days, good to go"),
            "{digest}"
        );
        assert!(
            !digest.contains("already caught up on"),
            "messages the watcher has already seen are not repeated: {digest}"
        );
    }

    /// The ordering the digest has to survive: the operator types at a **busy**
    /// child, so the text is a pending queue row and nothing is in the transcript
    /// yet. If the coordinator drains its catch-up in that gap — which it may,
    /// since the push is passive and a builder turn is long — the push retires and
    /// its read cursor advances, and nothing re-fires when the child finally
    /// records the message. Reading the pending queue is what keeps that from
    /// losing the operator's intervention permanently.
    #[tokio::test]
    async fn a_catchup_drained_before_the_child_reads_the_queue_still_names_the_message() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "also ship the trusted producer").await;

        create_catchup_pushes_for_watchers(&db, &child_node_uri(), None)
            .await
            .unwrap();
        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .expect("text the child has not read yet is still an operator intervention");

        assert!(
            digest.contains("also ship the trusted producer"),
            "{digest}"
        );
        assert!(
            digest.contains("queued, not yet read by the node"),
            "the digest distinguishes sent-and-read from sent-and-pending: {digest}"
        );

        // The coordinator drains before the child ever claims the queue: the push
        // retires and the cursor advances past the child's tail, so this delivery
        // was the only one that could have carried the message.
        let pushes = pending_catchup(&db, "watcher").await;
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert!(pending_catchup(&db, "watcher").await.is_empty());
        assert_eq!(
            read_cursor(&db, "watcher", "child-job").await.unwrap(),
            Some(1)
        );
    }

    /// The narrower window inside the claim itself. `claim_for_job_inner` commits
    /// the `delivered_at` stamp, and its caller inserts the transcript event
    /// afterwards — so between those two writes the text sits in neither a pending
    /// queue row nor the transcript. Selecting the queue on `delivered_at IS NULL`
    /// alone would blind the digest for exactly that gap, and a coordinator
    /// delivering inside it would again retire the only catch-up. Driven through
    /// the real claim rather than a hand-built state, since the ordering under test
    /// is the claim's own.
    #[tokio::test]
    async fn a_message_claimed_but_not_yet_recorded_is_still_named() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "also ship the trusted producer").await;

        let claimed = crate::messages::queued::claim_all_for_job_async(&db, "child-job")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1, "the child claimed the operator's text");
        assert_eq!(
            user_event_count(&db).await,
            0,
            "the claim commits before its caller records the transcript event"
        );

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .expect("text in flight between the queue and the transcript is not lost");

        assert!(
            digest.contains("also ship the trusted producer"),
            "{digest}"
        );
        assert!(
            !digest.contains("queued, not yet read by the node"),
            "a claimed row has been read by the node: {digest}"
        );
    }

    /// Repeated text is repeated emphasis, not a duplicate. The operator really
    /// does send the same short message twice, and two pending rows are two sends
    /// — neither can be in the transcript yet, so nothing about them is redundant.
    /// Content uniqueness across the digest would have hidden the second.
    #[tokio::test]
    async fn two_identical_operator_messages_are_two_messages() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "please stop").await;
        queue_operator_message(&db, "q-2", 310, "please stop").await;

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .unwrap();

        assert!(digest.contains("2 messages"), "{digest}");
        assert_eq!(digest.matches("please stop").count(), 2, "{digest}");
    }

    /// The same repetition once one copy has been claimed and recorded: the
    /// recorded copy cancels exactly one queue row, leaving the other send intact.
    #[tokio::test]
    async fn a_repeated_message_cancels_one_copy_not_both() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "please stop").await;
        crate::messages::queued::claim_all_for_job_async(&db, "child-job")
            .await
            .unwrap();
        add_user_message(&db, "u-1", 10, 305, "please stop").await;
        // A second send of the same text, still sitting in the queue.
        queue_operator_message(&db, "q-2", 310, "please stop").await;

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .unwrap();

        assert!(digest.contains("2 messages"), "{digest}");
        assert_eq!(digest.matches("please stop").count(), 2, "{digest}");
        assert_eq!(
            digest.matches("queued, not yet read by the node").count(),
            1,
            "only the unclaimed send is still unread: {digest}"
        );
    }

    /// Once the caller records the event, the same text is in both sources. It is
    /// one operator message and must read as one — and the two copies need not be
    /// adjacent in time, since a queued row carries the moment the operator typed
    /// it and its event the moment the node read it.
    #[tokio::test]
    async fn a_message_in_both_sources_is_named_once_not_twice() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "also ship the trusted producer").await;
        crate::messages::queued::claim_all_for_job_async(&db, "child-job")
            .await
            .unwrap();
        // A later, unrelated operator message lands between the two copies in time.
        add_user_message(&db, "u-mid", 10, 310, "and rerun the suite").await;
        add_user_message(&db, "u-1", 11, 320, "also ship the trusted producer").await;

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .unwrap();

        assert!(digest.contains("2 messages"), "{digest}");
        assert_eq!(
            digest.matches("also ship the trusted producer").count(),
            1,
            "non-adjacent duplicates still collapse: {digest}"
        );
    }

    #[tokio::test]
    async fn catchup_digest_is_bounded_in_count_and_in_length() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        for seq in 0..8i64 {
            add_user_message(
                &db,
                &format!("u-{seq}"),
                10 + seq,
                100 + seq,
                &format!("message {seq} {}", "x ".repeat(500)),
            )
            .await;
        }

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .unwrap();

        let lines: Vec<&str> = digest.lines().skip(1).collect();
        assert_eq!(lines.len(), CATCHUP_DIGEST_MESSAGES, "{digest}");
        assert!(
            digest.contains("message 7") && !digest.contains("message 2"),
            "the newest messages are the ones kept: {digest}"
        );
        for line in lines {
            assert!(
                line.chars().count() <= CATCHUP_DIGEST_CHARS + 32,
                "digest line is not bounded: {line}"
            );
        }
    }

    /// The digest leads the render, ahead of the chat window, because
    /// [`cap_push_content`] truncates from the front: for a busy child the window
    /// runs to dozens of turns and the operator's message — the newest thing in it
    /// — would be the first thing discarded.
    #[tokio::test]
    async fn rendered_catchup_leads_with_the_operator_digest() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_user_message(&db, "u-1", 10, 300, "scope change: also ship the producer").await;
        let content_ref = format!("{}/chat?offset=0", child_node_uri());
        let orch = test_orchestrator(db);
        let push = crate::orchestrator::attention_push::Push {
            id: "push-1".into(),
            recipient: "watcher".into(),
            content_ref: content_ref.clone(),
            wake: Wake::Passive,
            boundary: Boundary::Event,
            key: "catchup:child-job".into(),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(
            rendered.starts_with(&format!(
                "Attention update (passive): {content_ref}\n\nThe operator sent 1 message"
            )),
            "{rendered}"
        );
        assert!(
            rendered.contains("scope change: also ship the producer"),
            "{rendered}"
        );
    }

    #[tokio::test]
    async fn task_targeted_message_scopes_catchup_to_the_subtask_job() {
        use crate::orchestrator::attention_push::read_cursor;
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        // The node job (child-job) has 1 turn; the addressed sub-task job has 2.
        add_chat_turn(&db, "t1", 1).await;
        db.execute_script(
            "INSERT INTO jobs(id, project_id, issue_id, parent_job_id, uri_segment, status, current_session_id, created_at, updated_at)
               VALUES('task-job','p','issue-1','child-job','explore','running','sess3',1,1);
             INSERT INTO runs(id, project_id, job_id, issue_id, created_at, updated_at)
               VALUES('run-task','p','task-job','issue-1',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk1','sess3','run-task',1,'completed',1,1);
             INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
               VALUES('tk2','sess3','run-task',2,'completed',1,1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk1','run-task','tk1',1,1,'assistant','{}',1);
             INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
               VALUES('etk2','run-task','tk2',2,1,'assistant','{}',1);",
        )
        .await
        .unwrap();

        // A user message directed at the sub-task URI must scope catch-up to the
        // task's own chat, not the parent node's.
        let task_uri = format!("{CHILD_URI}/1/builder/task/explore");
        create_catchup_push(&db, "watcher", &task_uri)
            .await
            .unwrap();
        let pushes = pending_catchup(&db, "watcher").await;
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].key, "catchup:task-job");
        // Renders the task chat; start = task tail - 1 = 1 (task has 2 turns).
        assert!(
            pushes[0]
                .content_ref
                .ends_with("/task/explore/chat?offset=1"),
            "task-targeted catch-up renders the sub-task chat: {}",
            pushes[0].content_ref
        );
        deliver(&db, std::slice::from_ref(&pushes[0].id), "carry-1", 100).await;
        assert_eq!(
            read_cursor(&db, "watcher", "task-job").await.unwrap(),
            Some(2),
            "cursor is scoped to the sub-task job's turns"
        );
    }
}
