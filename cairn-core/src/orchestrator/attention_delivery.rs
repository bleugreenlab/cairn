//! Attention delivery engine (push-queue model, docs/attention-redesign.md).
//!
//! Creates and renders attention pushes. Two responsibilities:
//!
//! 1. **Create pushes.** [`create_resolved_push`] turns a terminal `Resolved`
//!    fact into a `resolved:{issue}` push to the issue's watchers. Terminal child
//!    resolution rouses its parent coordinator; mutes downgrade that push to a
//!    passive digest entry.
//!    [`create_catchup_push`] creates the passive `catchup:{child-job}` push at
//!    the user→child message moment, resolved at delivery against the parent's
//!    read cursor. Question and permission pushes are created at their own emit
//!    sites (where the producing node is known) through
//!    [`push_to_issue_watchers`].
//! 2. **Render pushes at resume time.** [`render_pushes_resolved`] gives every
//!    drained push a compact reference-first summary. Cheap, targeted database
//!    details are retained for direct messages, catch-up digests, and terminal
//!    resolutions; full resources are read by the agent only when needed.

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
/// node is known. Failed resolution wakes subscribed idle watchers; every terminal
/// resolution wakes the resolved child's parent coordinator. Other successful
/// resolution subscriptions remain passive. Supersede-by-key
/// collapses repeat undelivered emits, while a status/update fingerprint prevents
/// the same terminal resolution from re-firing after delivery. The resolved child
/// issue's own jobs are excluded so a child never receives its own notification.
pub(crate) fn create_resolved_push(orch: &Orchestrator, event: &AttentionEvent) {
    let final_status = match &event.fact {
        AttentionFact::Resolved { final_status } => final_status.clone(),
        _ => return,
    };
    let fingerprint = format!("status:{final_status}:{}", event.updated_at);
    let status_text = final_status.to_string();
    let dbs = orch.db.clone();
    let issue_id = event.issue_id.clone();
    let issue_uri = event.issue_uri.clone();
    let result = run_db_blocking(move || async move {
        let db = crate::issues::crud::owning_db_for_issue(&dbs, &issue_id)
            .await
            .map_err(|e| e.to_string())?;
        let key = format!("resolved:{issue_uri}");
        let watchers = crate::orchestrator::wakes::watcher_jobs_for_issue(&db, &issue_uri).await?;
        let parent_coordinator =
            crate::orchestrator::wakes::coordinating_job_for_child_issue(&db, &issue_uri).await?;
        let mut pushed = Vec::new();
        for recipient in watchers {
            if job_belongs_to_issue(&db, &recipient, &issue_id).await? {
                continue;
            }
            // This is the canonical moment a child becomes terminal for its
            // watchers, so it is where a thread learns that the turns which
            // discussed that child may now stand behind one chapter line. The
            // mark is eligibility only — nothing is rewritten here — and it is
            // idempotent, so re-delivery of the same terminal fact is a no-op.
            mark_thread_chapter(&db, &recipient, &issue_id, &issue_uri, &status_text).await;
            if let Some(Some(previous)) =
                super::attention_push::latest_push_fingerprint(&db, &recipient, &key)
                    .await
                    .map_err(|e| e.to_string())?
            {
                if previous == fingerprint {
                    continue;
                }
            }
            let requested_wake = if final_status == crate::models::IssueStatus::Failed
                || parent_coordinator.as_deref() == Some(recipient.as_str())
            {
                Wake::Wake
            } else {
                Wake::Passive
            };
            let (_, effective) = super::attention_push::push_with_fingerprint(
                &db,
                &recipient,
                &issue_uri,
                requested_wake,
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

/// Record that a terminal child makes a watching thread's turns compactable.
///
/// Best-effort by construction: compaction bookkeeping must never be able to
/// break attention delivery, so a failure is logged and the push proceeds. The
/// cost of a missed mark is one chapter's worth of turns staying verbatim until
/// the child is finalized again or the thread compacts on expiry.
async fn mark_thread_chapter(
    db: &LocalDb,
    recipient: &str,
    child_issue_id: &str,
    child_issue_uri: &str,
    final_status: &str,
) {
    if !crate::threads::compaction_capability_for_job(db, recipient)
        .await
        .is_enabled()
    {
        return;
    }
    let title = match db
        .query_opt_text(
            "SELECT title FROM issues WHERE id = ?1",
            params![child_issue_id.to_string()],
        )
        .await
    {
        Ok(Some(title)) => title,
        // A chapter with no overview is exactly the bare address the design
        // rejects, so fall back to the URI's own text rather than an empty line.
        _ => child_issue_uri.to_string(),
    };
    let mark = crate::threads::compaction::ChildMark {
        child_issue_id: child_issue_id.to_string(),
        child_issue_uri: child_issue_uri.to_string(),
        child_title: title,
        final_status: final_status.to_string(),
        marked_at: chrono::Utc::now().timestamp(),
    };
    if let Err(error) = crate::threads::compaction::mark_child_terminal(db, recipient, &mark).await
    {
        log::warn!(
            "thread chapter mark for {} on {child_issue_uri} failed: {error}",
            &recipient[..recipient.len().min(8)]
        );
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
        parsed.project().map(cairn_common::uri::canonical_project),
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
    let project = cairn_common::uri::canonical_project(project);
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

/// How many operator messages a catch-up digest names, and how much of each.
const CATCHUP_DIGEST_MESSAGES: usize = 5;
const CATCHUP_DIGEST_CHARS: usize = 240;

/// The operator messages a watcher has not caught up on for `child_job_id`, as a
/// bounded digest line per message (CAIRN-3342).
///
/// This exists because the reference alone cannot carry the fact reliably. A
/// busy child's unread chat window can run to dozens of turns, while the newest
/// operator message is the load-bearing reason for the notification. The digest
/// carries that bounded fact inline and the URI supplies context on demand.
///
/// The one source is `queued_messages`, because it is the one durable record
/// whose every row is text a human typed at this job: the composer's queued
/// follow-up, and the idle-child send that goes straight into a resume (recorded
/// delivered on arrival). The child's own transcript cannot serve here. A `user`
/// event is a *role*, not an author — the job's launch prompt, a delegated
/// artifact payload delivered to a review node, and every machinery resume land
/// in the same slot as the operator's typed `^`, distinguishable only by reading
/// their content. Scanning that role told coordinators "the operator sent" over
/// issue bodies they had written themselves (CAIRN-3390), which is both noise a
/// long-lived thread pays for forever and a false claim about who spoke. What
/// cannot be established as operator-authored is therefore left out rather than
/// guessed at; the canonical chat-window URI still supplies that context on
/// demand.
///
/// A row is named while it is still **pending** — the node has not read it, so it
/// stays live information for as long as that holds — or when it was sent at or
/// after the recipient's read cursor `updated_at`, which is stamped in the same
/// transaction that delivered its last catch-up. Reading the queue rather than
/// waiting for the transcript also tells the coordinator sooner: text typed at a
/// busy child sits in the queue until the child reaches a tool boundary or turn
/// end, which can be a whole builder turn later, and a coordinator running inside
/// that gap would otherwise render an empty digest, retire the push, and advance
/// its cursor past the child's tail with nothing left to re-fire.
///
/// With no cursor yet (a first catch-up), the newest [`CATCHUP_DIGEST_MESSAGES`]
/// stand in. Rows are read at delivery, so an edited or deleted queued message
/// reports its current state rather than a stale copy.
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
    let messages: Vec<OperatorMessage> = db
        .read(|conn| {
            let child_job_id = child_job_id.clone();
            Box::pin(async move {
                let mut messages = Vec::new();
                let mut rows = conn
                    .query(
                        "SELECT created_at, content, delivered_at FROM queued_messages
                         WHERE job_id = ?1
                           AND (delivered_at IS NULL OR created_at >= ?2)
                         ORDER BY created_at DESC
                         LIMIT ?3",
                        params![
                            child_job_id.as_str(),
                            anchor,
                            CATCHUP_DIGEST_MESSAGES as i64
                        ],
                    )
                    .await?;
                while let Some(row) = rows.next().await? {
                    let content = row.text(1)?;
                    if !content.trim().is_empty() {
                        messages.push(OperatorMessage {
                            timestamp: row.i64(0)?,
                            content,
                            read_by_node: row.opt_i64(2)?.is_some(),
                        });
                    }
                }
                Ok(messages)
            })
        })
        .await
        .ok()?;

    if messages.is_empty() {
        return None;
    }
    let count = messages.len();
    let noun = if count == 1 { "message" } else { "messages" };
    let lines: Vec<String> = messages
        .iter()
        .rev()
        .map(|message| {
            let when = crate::clock::stamp(message.timestamp)
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

/// One operator message bound for a child node.
struct OperatorMessage {
    timestamp: i64,
    content: String,
    /// `false` while the text is still a pending `queued_messages` row the child
    /// has not claimed — sent by the operator, not yet seen by the node.
    read_by_node: bool,
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
                crate::models::IssueStatus::Merged => "merged",
                crate::models::IssueStatus::Closed => "closed",
                crate::models::IssueStatus::Failed => {
                    return Ok(Some(format!(
                        "{}-{} \"{}\" failed — inspect the child and retry or delegate a fix",
                        issue.project_key, issue.number, issue.title
                    )))
                }
                _ => return Ok(None),
            };
            let mut rows = conn
                .query(
                    "SELECT mr.github_pr_number, mr.status, a.action, a.actor_kind, a.actor_identity
                     FROM merge_requests mr
                     LEFT JOIN pr_resolution_attributions a ON a.id = (
                         SELECT a2.id FROM pr_resolution_attributions a2
                         WHERE a2.merge_request_id = mr.id
                         ORDER BY a2.created_at DESC LIMIT 1
                     )
                     WHERE mr.issue_id = ?1
                     ORDER BY mr.updated_at DESC LIMIT 1",
                    params![issue.issue_id.as_str()],
                )
                .await?;
            let pr = match rows.next().await? {
                Some(row) => {
                    let number = row.opt_i64(0)?;
                    let pr_status = row.text(1)?;
                    let action = row.opt_text(2)?;
                    let actor_kind = row.opt_text(3)?;
                    let actor_identity = row.opt_text(4)?;
                    let action_matches_state = matches!(
                        (message, pr_status.as_str(), action.as_deref()),
                        ("merged", "merged", Some("merge")) | ("closed", "closed", Some("close"))
                    );
                    number
                        .filter(|number| *number > 0 && action_matches_state)
                        .map(|number| {
                            let actor = match (actor_kind.as_deref(), actor_identity.as_deref()) {
                                (Some("operator-ui"), _) => "operator (UI)".to_string(),
                                (Some("operator-cli"), _) => "operator (CLI)".to_string(),
                                (_, Some(identity)) => identity.to_string(),
                                (Some(kind), _) => kind.to_string(),
                                _ => "unknown actor".to_string(),
                            };
                            format!(" PR #{number}, by {actor}")
                        })
                }
                None => None,
            }
            .unwrap_or_default();
            Ok(Some(format!(
                "{}-{} \"{}\" {message}.{pr}",
                issue.project_key, issue.number, issue.title
            )))
        })
    })
    .await
    .ok()
    .flatten()
}

/// Render a drained attention push as a compact, reference-first update.
///
/// Direct messages, catch-up digests, terminal resolutions, and check verdicts
/// have bounded, targeted database lookups because their essential detail is not
/// represented by the resource URI alone. Other push kinds deliberately do not
/// render their full resource here: resume prompt assembly holds the launch lock,
/// and making a resource read per pending push made user messages wait tens of
/// seconds. The URI remains the canonical route to the live content through the
/// read tool.
pub(crate) async fn render_push_resolved(
    orch: &Orchestrator,
    push: &crate::orchestrator::attention_push::Push,
) -> String {
    let header = format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    );
    if push.key == "build-change" {
        return match super::build_change::resolve(&orch.db.local, &push.content_ref).await {
            Ok(Some(change)) => super::build_change::render(&change),
            _ => header,
        };
    }
    if push.key.starts_with("resolved:") {
        return match resolved_issue_confirmation(orch, &push.content_ref).await {
            Some(body) => format!("{header}\n\n{body}"),
            None => header,
        };
    }

    // A `turn-checks:` push exists to say that a check went red, and a verdict is
    // short. Waking an agent only to send it somewhere else to find out why costs
    // a whole turn to learn one sentence, so the verdicts are rendered here from
    // the job's own recorded rows (CAIRN-3848). Deliberately scoped to the
    // `turn-checks:` prefix: the sibling `turn-checks-infrastructure:` push goes
    // to the PARENT, whose own rows are not the ones it reports on.
    if push.key.starts_with("turn-checks:") {
        if let Some(body) =
            crate::execution::checks_status::check_wake_body(orch, &push.recipient).await
        {
            return format!("{header}\n\n{body}");
        }
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
    // A `catchup:` push carries the operator-message digest without rendering the
    // potentially enormous chat window. The URI remains available for follow-up.
    let digest = match push.key.strip_prefix("catchup:") {
        Some(child_job_id) => {
            let db = crate::execution::routing::owning_db_for_job(&orch.db, child_job_id)
                .await
                .unwrap_or_else(|_| orch.db.local.clone());
            catchup_operator_digest(&db, &push.recipient, child_job_id).await
        }
        None => None,
    };
    if let Some(digest) = digest {
        return format!("{header}\n\n{digest}");
    }
    let prefix = push
        .key
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(&push.key);
    let (_, headline) = super::attention_push::push_kind_headline(prefix);
    format!(
        "{header}\n\n{headline}. Read {} for the current content.",
        push.content_ref
    )
}

/// Render several pushes concurrently into one compact block, or `None` when the
/// slice is empty so callers can fold it into an optional prompt section.
pub(crate) async fn render_pushes_resolved(
    orch: &Orchestrator,
    pushes: &[crate::orchestrator::attention_push::Push],
) -> Option<String> {
    if pushes.is_empty() {
        return None;
    }
    use futures_util::{stream, StreamExt};

    let mut blocks = stream::iter(pushes.iter().enumerate())
        .map(|(index, push)| async move { (index, render_push_resolved(orch, push).await) })
        .buffer_unordered(16)
        .collect::<Vec<_>>()
        .await;
    blocks.sort_by_key(|(index, _)| *index);
    let blocks = blocks
        .into_iter()
        .map(|(_, block)| block)
        .collect::<Vec<_>>();
    Some(blocks.join("\n\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::LocalDb;

    const CHILD_URI: &str = "cairn://p/proj/2";

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
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
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
    async fn ordinary_push_is_reference_first_without_rendering_the_resource() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "UPDATE issues SET description='expensive body sentinel' WHERE id='issue-1';",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let uri = "cairn://p/proj/2/1/builder/create-pr";
        let push = crate::orchestrator::attention_push::Push {
            id: "push-review".into(),
            recipient: "watcher".into(),
            content_ref: uri.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: "review:child-job".into(),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(
            rendered.contains("Work product ready for review"),
            "{rendered}"
        );
        assert!(rendered.contains(&format!("Read {uri}")), "{rendered}");
        assert!(!rendered.contains("expensive body sentinel"), "{rendered}");
    }

    /// The delivery seam for CAIRN-3848: a checks wake carries the verdicts, so
    /// the agent it rouses learns which lane went red and what failed in it
    /// rather than being sent to a resource to find out.
    #[tokio::test]
    async fn checks_push_renders_the_verdicts_instead_of_a_pointer() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            r#"
            INSERT INTO check_result_cache
              (project_id, tree_hash, input_hash, check_name, exit_code, passed,
               output_tail, duration_ms, ran_at, target_results_json, job_id)
            VALUES
              ('p','tree','ih-rust','rust-tests',101,0,'',1000,1,
               '{"parser":"nextest","passed":64,"failed":2,"skipped":0,
                 "failures":[{"name":"cairn_core a::b","message":null},
                             {"name":"cairn_core c::d","message":null}]}',
               'child-job'),
              ('p','tree','ih-fmt','rust-fmt',0,1,'',10,1,NULL,'child-job');
            "#,
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let uri = "cairn://p/proj/2/1/builder/checks";
        let push = crate::orchestrator::attention_push::Push {
            id: "push-checks".into(),
            recipient: "child-job".into(),
            content_ref: uri.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: format!("turn-checks:{uri}"),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(
            rendered.contains("✗ rust-tests — 2 of 66 failed: cairn_core a::b, cairn_core c::d"),
            "{rendered}"
        );
        assert!(rendered.contains("✓ passing: rust-fmt"), "{rendered}");
        assert!(
            !rendered.contains("for the current content"),
            "a wake must not spend a turn telling the agent to go read: {rendered}"
        );
    }

    #[tokio::test]
    async fn compact_push_batch_preserves_delivery_order() {
        let orch = test_orchestrator(migrated_db().await);
        let make_push = |id: &str, uri: &str| crate::orchestrator::attention_push::Push {
            id: id.into(),
            recipient: "watcher".into(),
            content_ref: uri.into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: format!("review:{id}"),
            created_at: 1,
            delivered_event_id: None,
        };
        let pushes = vec![
            make_push("first", "cairn://p/proj/1/1/first/create-pr"),
            make_push("second", "cairn://p/proj/2/1/second/create-pr"),
            make_push("third", "cairn://p/proj/3/1/third/create-pr"),
        ];

        let rendered = render_pushes_resolved(&orch, &pushes).await.unwrap();

        let first = rendered.find(&pushes[0].content_ref).unwrap();
        let second = rendered.find(&pushes[1].content_ref).unwrap();
        let third = rendered.find(&pushes[2].content_ref).unwrap();
        assert!(first < second && second < third, "{rendered}");
    }

    #[tokio::test]
    async fn direct_push_keeps_frozen_message_content_inline() {
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO messages
               (id, channel_type, channel_id, sender_run_id, sender_name,
                recipient_run_id, content, created_at)
             VALUES
               ('direct-message', 'direct', NULL, NULL,
                'cairn://p/proj/1/1/sender', NULL, 'frozen direct content', 1);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);
        let push = crate::orchestrator::attention_push::Push {
            id: "direct-push".into(),
            recipient: "watcher".into(),
            content_ref: "cairn://p/proj/1/1/watcher/messages".into(),
            wake: Wake::Wake,
            boundary: Boundary::Event,
            key: "direct:direct-message".into(),
            created_at: 1,
            delivered_event_id: None,
        };

        let rendered = render_push_resolved(&orch, &push).await;

        assert!(rendered.contains("frozen direct content"), "{rendered}");
        assert!(rendered.contains("sender/messages"), "{rendered}");
    }

    #[tokio::test]
    async fn render_resolved_push_uses_concise_confirmation_not_issue_body() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "UPDATE issues
             SET status='merged', description='This long child issue description should not be inlined.'
             WHERE id='issue-1';
             INSERT INTO merge_requests
               (id, job_id, project_id, issue_id, title, source_branch, target_branch,
                status, opened_at, updated_at, github_pr_number)
             VALUES
               ('mr-child', 'child-job', 'p', 'issue-1', 'Child PR', 'child', 'main',
                'merged', 1, 2, 42);
             INSERT INTO pr_resolution_attributions
               (id, merge_request_id, action, actor_kind, surface, lane_snapshot, created_at)
             VALUES
               ('attr-child', 'mr-child', 'merge', 'operator-ui', 'operator-ui', '{}', 2);",
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

        assert!(rendered.contains("Attention update (passive): cairn://p/proj/2"));
        assert!(rendered.contains("proj-2 \"Child\" merged"), "{rendered}");
        assert!(rendered.contains("PR #42, by operator (UI)"), "{rendered}");
        assert!(!rendered.contains("Description"));
        assert!(!rendered.contains("This long child issue description"));

        orch.db
            .local
            .execute(
                "UPDATE merge_requests SET status='open' WHERE id='mr-child'",
                (),
            )
            .await
            .unwrap();
        let mismatched = render_push_resolved(&orch, &push).await;
        assert!(
            mismatched.contains("proj-2 \"Child\" merged"),
            "{mismatched}"
        );
        assert!(!mismatched.contains("PR #42"), "{mismatched}");
        assert!(!mismatched.contains("operator (UI)"), "{mismatched}");
    }

    #[tokio::test]
    async fn render_failed_resolution_routes_to_team_replica() {
        let local = migrated_db().await;
        let team = std::sync::Arc::new(migrated_db().await);
        team.execute_script(
            "
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('team-project','default','Team Project','team','/tmp/team',1,1);
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
        orch.db.set_route("team", Some("team-1".to_string())).await;
        let issue_uri = "cairn://p/team/9";
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

        assert!(
            rendered.contains("team-9 \"Failed child\" failed"),
            "{rendered}"
        );
        assert!(rendered.contains("retry or delegate a fix"), "{rendered}");
        assert!(!rendered.contains("Successfully"), "{rendered}");
    }

    #[tokio::test]
    async fn render_unconfirmed_resolution_is_bare_header() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        let orch = test_orchestrator(db);

        for issue_uri in [CHILD_URI, "cairn://p/proj/999"] {
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
            "cairn://p/proj/2/1/planner/questions/q-1",
            Wake::Wake,
            Boundary::Event,
            "question:cairn://p/proj/2",
        )
        .await
        .unwrap();

        let watcher = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].boundary, Boundary::Event);
        assert_eq!(watcher[0].key, "question:cairn://p/proj/2");
        assert_eq!(
            watcher[0].content_ref,
            "cairn://p/proj/2/1/planner/questions/q-1"
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
            "cairn://p/proj/2/1/planner/plan",
            Wake::Wake,
            Boundary::Event,
            &format!("review:{CHILD_URI}"),
        )
        .await
        .unwrap();

        let watcher = list_pending(&db, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1, "the coordinating node receives the gate");
        assert_eq!(watcher[0].wake, Wake::Wake);
        assert_eq!(watcher[0].content_ref, "cairn://p/proj/2/1/planner/plan");
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
               VALUES('seeded','watcher','issue','cairn://p/proj/2',
                      '[\"message\",\"permission\",\"question\",\"resolved\",\"review\"]','muted','system',1,1,0);",
        )
        .await
        .unwrap();
        let key = format!("review:{CHILD_URI}");

        push_to_issue_watchers(
            &db,
            CHILD_URI,
            Some("child-job"),
            "cairn://p/proj/2/1/planner/plan",
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
            "cairn://p/proj/2/1/planner/plan",
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
    async fn terminal_child_resolution_wakes_parent_without_subscription_row() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        for final_status in [IssueStatus::Merged, IssueStatus::Closed] {
            let db = migrated_db().await;
            seed(&db, "active", None, None).await;
            db.execute_script(
                "DELETE FROM wake_subscriptions;
                 UPDATE issues SET parent_issue_id = 'parent' WHERE id = 'issue-1';",
            )
            .await
            .unwrap();
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
                    route_provenance: None,
                },
            );

            let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
            assert_eq!(watcher.len(), 1);
            assert_eq!(watcher[0].wake, Wake::Wake);
            assert_eq!(watcher[0].key, format!("resolved:{CHILD_URI}"));
            assert_eq!(watcher[0].content_ref, CHILD_URI);
        }
    }

    #[tokio::test]
    async fn terminal_thread_child_wakes_the_session_and_marks_its_chapter() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        db.execute_script(
            "INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
             INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
               VALUES('p','w','Project','proj','/tmp/repo',1,1);
             INSERT INTO threads(id, project_id, name, status, attention, created_at, updated_at)
               VALUES('thread','p','general','active','none',1,1);
             INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_thread_id, created_at, updated_at)
               VALUES('issue-1','p',2,'Child','merged','complete','none','thread',1,2);",
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Merged,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Merged,
                updated_at: 2,
                route_provenance: None,
            },
        );

        let job_id = orch
            .db
            .local
            .query_opt_text("SELECT id FROM jobs WHERE thread_id='thread'", ())
            .await
            .unwrap()
            .expect("terminal attention establishes the dormant thread session");
        let pushes = list_pending(&orch.db.local, &job_id).await.unwrap();
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].wake, Wake::Wake);
        assert_eq!(
            orch.db
                .local
                .query_opt_i64(
                    "SELECT COUNT(*) FROM thread_compaction_marks WHERE job_id=?1 AND child_issue_id='issue-1'",
                    params![job_id],
                )
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn muted_terminal_child_resolution_rides_parent_digest() {
        use crate::models::{IssueAttention, IssueStatus};
        use crate::orchestrator::attention::{AttentionEvent, AttentionFact};
        use crate::orchestrator::attention_push::list_pending;
        let db = migrated_db().await;
        seed(&db, "muted", Some(r#"["resolved"]"#), None).await;
        db.execute(
            "UPDATE issues SET parent_issue_id = 'parent' WHERE id = 'issue-1'",
            (),
        )
        .await
        .unwrap();
        let orch = test_orchestrator(db);

        super::create_resolved_push(
            &orch,
            &AttentionEvent {
                issue_id: "issue-1".into(),
                issue_uri: CHILD_URI.into(),
                fact: AttentionFact::Resolved {
                    final_status: IssueStatus::Merged,
                },
                attention: IssueAttention::None,
                status: IssueStatus::Merged,
                updated_at: 1,
                route_provenance: None,
            },
        );

        let watcher = list_pending(&orch.db.local, "watcher").await.unwrap();
        assert_eq!(watcher.len(), 1);
        assert_eq!(watcher[0].wake, Wake::Passive);
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
                route_provenance: None,
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
                route_provenance: None,
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
                route_provenance: None,
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

    /// End to end on the shape that broke (CAIRN-3712): a `plan>coordinator`
    /// graph whose two nodes were minted in one pass and share a `created_at`. A
    /// user message on a child the coordinator spawned addresses the catch-up to
    /// the COORDINATOR; the upstream planner is not woken for work it never
    /// delegated.
    #[tokio::test]
    async fn child_catchup_reaches_the_coordinator_that_spawned_it() {
        let db = migrated_db().await;
        db.execute_script(
            "
            INSERT INTO workspaces(id, name, created_at, updated_at) VALUES('w','W',1,1);
            INSERT INTO projects(id, workspace_id, name, key, repo_path, created_at, updated_at)
              VALUES('p','w','Project','proj','/tmp/repo',1,1);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, created_at, updated_at)
              VALUES('parent','p',1,'Parent','active','active','none',1,1);
            INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
              VALUES('exec-parent','plan-coordinator','parent','p','running',1,1);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('planner','p','parent','exec-parent','planner','complete','sess-planner',5,5);
            INSERT INTO jobs(id, project_id, issue_id, execution_id, uri_segment, status, current_session_id, created_at, updated_at)
              VALUES('coordinator','p','parent','exec-parent','coordinator','running','sess-coord',5,5);
            INSERT INTO issues(id, project_id, number, title, status, progress, attention, parent_issue_id, parent_job_id, created_at, updated_at)
              VALUES('issue-1','p',2,'Child','active','active','none','parent','coordinator',1,1);
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
        add_chat_turn(&db, "t1", 1).await;

        assert_eq!(
            super::create_catchup_pushes_for_watchers(&db, &child_node_uri(), None)
                .await
                .unwrap(),
            1,
            "exactly one node is caught up on the child"
        );
        assert_eq!(pending_catchup(&db, "coordinator").await.len(), 1);
        assert!(
            pending_catchup(&db, "planner").await.is_empty(),
            "the planner receives no catch-up for a child it did not spawn"
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

    /// The child job's own launch prompt: the issue description its coordinator
    /// wrote, stored as the job's first `user` event by `prepare_job`. Verbatim
    /// head of the CAIRN-3386 builder's launch event.
    const LAUNCH_PROMPT: &str = "# Check cells fail preparation: `bun i` exits 127, bun not on the PATH the executor gives setup commands\n\nEvery rust-lint/rust-tests review check since the 2026-08-01 05:17 UTC rebuild reports 'Cairn could not prepare a working environment for this check.'";

    /// A delegated artifact payload delivered into a review node's user slot by
    /// the delegation runtime. Verbatim head of the CAIRN-3381 review node's
    /// delivery event; hundreds of rows in the live database share the shape.
    const ARTIFACT_PAYLOAD: &str = "**create-pr**\n\n```json\n{\n  \"body\": \"## What was wrong\\n\\n`cairn check run <suite>` refused from an agent shell before doing anything.\",\n  \"title\": \"check: pass CAIRN_RUN_ID into agent shells\"\n}\n```";

    /// A machinery resume: Cairn's own note to a node, carried into the same
    /// user slot by `continue_job_impl` (`wake_upstream_after_checkpoint_failure`).
    const MACHINERY_RESUME: &str =
        "Checkpoint `checks` failed (exit 1) running `bun run check:rust`.";

    /// Insert a `user` transcript event on the child job's run, in the shape
    /// production stores it — serialized from the same struct the storage path
    /// builds, so the fixture cannot drift from what actually lands in `data`.
    async fn add_transcript_user_event(
        db: &LocalDb,
        id: &str,
        seq: i64,
        timestamp: i64,
        content: &str,
    ) {
        let id = id.to_string();
        let data = serde_json::to_string(&crate::agent_process::stream::TranscriptEvent {
            event_type: "user".to_string(),
            session_id: Some("sess2".to_string()),
            parent_tool_use_id: None,
            content: Some(content.to_string()),
            thinking: None,
            tool_name: None,
            tool_input: None,
            tool_uses: None,
            tool_use_id: None,
            tool_result: None,
            is_error: false,
            thinking_ms: None,
            queued_message_id: None,
            raw: None,
        })
        .unwrap();
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
        insert_operator_message(db, id, created_at, None, content).await;
    }

    /// Operator text that reached an idle child directly: recorded delivered on
    /// arrival, because the resume it triggered carried it to the agent itself.
    async fn record_operator_send(db: &LocalDb, id: &str, created_at: i64, content: &str) {
        insert_operator_message(db, id, created_at, Some(created_at), content).await;
    }

    async fn insert_operator_message(
        db: &LocalDb,
        id: &str,
        created_at: i64,
        delivered_at: Option<i64>,
        content: &str,
    ) {
        let id = id.to_string();
        let content = content.to_string();
        db.write(move |conn| {
            let id = id.clone();
            let content = content.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO queued_messages(id, job_id, content, delivery, created_at, delivered_at)
                     VALUES(?1,'child-job',?2,'queue',?3,?4)",
                    params![id.as_str(), content.as_str(), created_at, delivered_at],
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
    /// `issues.parent_job_id` still names the retired spawner, and it is still
    /// perfectly resumable — what disqualifies it is that its execution is no
    /// longer the parent's latest, so the child moves to the coordinator driving
    /// the parent now.
    ///
    /// Supersession is an execution fact, not a recency one: a newer sibling root
    /// job alone must never retire a spawner, because every node of one graph is
    /// minted together and the coordinator's own upstream planner is such a
    /// sibling.
    #[tokio::test]
    async fn operator_message_catchup_reaches_the_live_coordinator_not_the_spawner() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        db.execute_script(
            "DELETE FROM wake_subscriptions;
             UPDATE issues SET parent_issue_id='parent', parent_job_id='watcher' WHERE id='issue-1';
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('exec-retired','r','parent','p','complete',1,1);
             INSERT INTO executions(id, recipe_id, issue_id, project_id, status, started_at, seq)
               VALUES('exec-live','r','parent','p','running',5,2);
             UPDATE jobs SET execution_id='exec-retired' WHERE id='watcher';
             INSERT INTO jobs(id, project_id, issue_id, execution_id, status, current_session_id, created_at, updated_at)
               VALUES('successor','p','parent','exec-live','running','sess3',5,5);",
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
        record_operator_send(&db, "q-old", 100, "this one was already caught up on").await;
        record_operator_send(&db, "q-new", 300, "queue drained for days, good to go").await;
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

    /// The claim is the node reading the message, not the watcher hearing about
    /// it: a claimed row stays in the digest until the watcher's own read cursor
    /// passes it, and stops being labelled as unread. Driven through the real
    /// claim rather than a hand-built state, since the transition under test is
    /// the claim's own.
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

    /// The same repetition once one copy has been read: two sends are two
    /// messages, and only the one the node has not reached is labelled unread.
    #[tokio::test]
    async fn a_repeated_message_is_named_once_per_send() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        queue_operator_message(&db, "q-1", 300, "please stop").await;
        crate::messages::queued::claim_all_for_job_async(&db, "child-job")
            .await
            .unwrap();
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

    /// The CAIRN-3390 specimen set. Everything Cairn delivers to a node lands in
    /// the same `user` slot the operator's own typing does — the job's launch
    /// prompt (the issue description its coordinator wrote), a delegated artifact
    /// payload handed to a review node, a machinery resume note — so a digest
    /// built from that slot told each coordinator "the operator sent" over its own
    /// words. Authorship, not role, decides: only the send recorded in the
    /// operator-message record is named.
    #[tokio::test]
    async fn machinery_user_events_are_never_attributed_to_the_operator() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_transcript_user_event(&db, "u-launch", 10, 300, LAUNCH_PROMPT).await;
        add_transcript_user_event(&db, "u-payload", 11, 310, ARTIFACT_PAYLOAD).await;
        add_transcript_user_event(&db, "u-machinery", 12, 320, MACHINERY_RESUME).await;
        // The operator resumes the child past its session limit. The transcript
        // event this stores is indistinguishable from the three above; the record
        // of who typed it is the row.
        add_transcript_user_event(&db, "u-operator", 13, 330, "^").await;
        record_operator_send(&db, "q-1", 330, "^").await;

        let digest = catchup_operator_digest(&db, "watcher", "child-job")
            .await
            .expect("the operator's own message is still named");

        assert!(digest.contains("1 message"), "{digest}");
        assert!(digest.ends_with("— ^"), "{digest}");
        assert!(
            !digest.contains("Check cells fail preparation"),
            "a coordinator must not be told it sent the issue body it wrote: {digest}"
        );
        assert!(
            !digest.contains("create-pr"),
            "a delegated artifact payload is not an operator message: {digest}"
        );
        assert!(
            !digest.contains("Checkpoint"),
            "a machinery resume note is not an operator message: {digest}"
        );
    }

    /// The attribution half of the same rule: with nothing operator-authored to
    /// report, the digest is absent rather than a header over machinery text. The
    /// chat window that follows still carries those turns as context.
    #[tokio::test]
    async fn a_child_with_only_machinery_user_events_has_no_digest() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        add_transcript_user_event(&db, "u-launch", 10, 300, LAUNCH_PROMPT).await;
        add_transcript_user_event(&db, "u-payload", 11, 310, ARTIFACT_PAYLOAD).await;

        assert!(
            catchup_operator_digest(&db, "watcher", "child-job")
                .await
                .is_none(),
            "nothing the operator authored means no operator digest"
        );
    }

    #[tokio::test]
    async fn catchup_digest_is_bounded_in_count_and_in_length() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        for seq in 0..8i64 {
            queue_operator_message(
                &db,
                &format!("q-{seq}"),
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
            let (_, excerpt) = line
                .split_once(" — ")
                .expect("a digest line has an excerpt");
            assert!(
                excerpt.chars().count() <= CATCHUP_DIGEST_CHARS + 1,
                "digest excerpt is not bounded: {excerpt}"
            );
        }
    }

    /// Catch-up carries the bounded operator-message digest inline and leaves the
    /// potentially enormous chat window behind its canonical URI for an on-demand
    /// read.
    #[tokio::test]
    async fn rendered_catchup_leads_with_the_operator_digest() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_chat_turn(&db, "t1", 1).await;
        record_operator_send(&db, "q-1", 300, "scope change: also ship the producer").await;
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

    /// Insert the child job's launch event on its opening turn, in the shape
    /// production stores it now that launch prompts are namespaced.
    async fn add_launch_event(db: &LocalDb, turn_id: &str, seq: i64, content: &str) {
        let turn_id = turn_id.to_string();
        let data = serde_json::json!({
            "eventType": crate::transcripts::LAUNCH_EVENT_TYPE,
            "sessionId": "sess2",
            "content": content,
        })
        .to_string();
        db.write(move |conn| {
            let turn_id = turn_id.clone();
            let data = data.clone();
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO turns(id, session_id, run_id, sequence, state, created_at, updated_at)
                     VALUES(?1,'sess2','run-1',?2,'completed',1,1)",
                    params![turn_id.as_str(), seq],
                )
                .await?;
                conn.execute(
                    "INSERT INTO events(id, run_id, turn_id, sequence, timestamp, event_type, data, created_at)
                     VALUES(?1,'run-1',?2,?3,1,?4,?5,1)",
                    params![
                        format!("launch-{turn_id}"),
                        turn_id.as_str(),
                        seq,
                        crate::transcripts::LAUNCH_EVENT_TYPE,
                        data.as_str()
                    ],
                )
                .await?;
                Ok::<(), crate::storage::DbError>(())
            })
        })
        .await
        .unwrap();
    }

    /// The CAIRN-3408 specimen, end to end through the surface that produced it.
    ///
    /// A thread files a child, so the child's launch prompt IS the thread's own
    /// issue description. The thread then gets a passive catch-up whose window
    /// opens at turn 1 (`offset=0`, what a brand-new child's cursor resolves to).
    /// What must come back is the operator's message and no trace of the body the
    /// recipient itself wrote — under threads that echo is a per-turn context tax
    /// paid forever.
    #[tokio::test]
    async fn rendered_catchup_never_echoes_the_recipients_own_launch_prompt() {
        let db = migrated_db().await;
        seed(&db, "active", None, None).await;
        add_launch_event(&db, "t1", 1, LAUNCH_PROMPT).await;
        add_chat_turn(&db, "t2", 2).await;
        record_operator_send(&db, "q-1", 300, "u dont need to cd prefix").await;
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
            rendered.contains("u dont need to cd prefix"),
            "the one genuinely operator-authored message must arrive: {rendered}"
        );
        let launch_head = LAUNCH_PROMPT.lines().next().unwrap();
        assert!(
            !rendered.contains(launch_head),
            "the recipient authored this launch prompt; catch-up must not quote it back: {rendered}"
        );
        assert!(
            !rendered.contains("**User:**"),
            "nothing in this window was typed by a user: {rendered}"
        );
        assert!(!rendered.contains(crate::transcripts::LAUNCH_MARKER_LINE));
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
