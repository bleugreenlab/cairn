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
//!    details are retained for direct messages, catch-up digests, terminal
//!    resolutions, check verdicts, and posts — the referents whose point is lost
//!    when only their address arrives; full resources are read by the agent only
//!    when needed.

use cairn_db::turso::params;

use super::push::{Boundary, Wake};
use crate::events::{AttentionEvent, AttentionFact};
use crate::runtime::{AttentionRuntime, ContentRequest};
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
/// collapses repeat undelivered emits, while a status/generation fingerprint
/// prevents the same terminal resolution from re-firing after delivery. The
/// resolved child issue's own jobs are excluded so a child never receives its own
/// notification.
pub fn create_resolved_push(runtime: &AttentionRuntime, event: &AttentionEvent) {
    let final_status = match &event.fact {
        AttentionFact::Resolved { final_status } => final_status.clone(),
        _ => return,
    };
    let status_text = final_status.to_string();
    let host = runtime.host.clone();
    let issue_id = event.issue_id.clone();
    let issue_uri = event.issue_uri.clone();
    let updated_at = event.updated_at;
    let result = run_db_blocking(move || async move {
        let db = host
            .database_for_issue(&issue_id)
            .await
            .map_err(|e| e.to_string())?;
        let fingerprint = format!(
            "status:{final_status}:{}",
            resolution_key(&db, &issue_id, &final_status, updated_at).await
        );
        let key = format!("resolved:{issue_uri}");
        let watchers = crate::wakes::watcher_jobs_for_issue(host.clone(), &db, &issue_uri).await?;
        let parent_coordinator =
            crate::wakes::coordinating_job_for_child_issue(host.clone(), &db, &issue_uri).await?;
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
            host.mark_thread_chapter(&db, &recipient, &issue_id, &issue_uri, &status_text)
                .await;
            if let Some(Some(previous)) =
                super::push::latest_push_fingerprint(&db, &recipient, &key)
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
            let (_, effective) = super::push::push_with_fingerprint(
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
            runtime.host.emit_change("attention_pushes", None);
            for recipient in recipients {
                if let Err(e) = runtime
                    .host
                    .nudge(&recipient, cairn_models::DeliveryUrgency::Steer)
                {
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

/// Which resolution a resolved push announces — the identity its fingerprint is
/// keyed to, as `{instant}:{generation}`.
///
/// This has to be a property of the RESOLUTION, not of the issue row. The
/// fingerprint used `issues.updated_at`, which is a mutation clock: any write
/// touching the issue minted a fresh fingerprint for an unchanged fact, so the
/// at-most-once guard never matched and a settled resolution could be pushed
/// again (CAIRN-4756).
///
/// Both halves are load-bearing, for opposite failure modes.
///
/// The **instant** (`merged_at` / `closed_at` / `completed_at`) is written once
/// by the transition that resolved the issue and cleared by `unresolve`, so it
/// is unmoved by any number of recomputes. That is what stops the replay.
///
/// The **generation** is what makes a genuine re-resolution distinguishable. The
/// instants are whole seconds, so an issue merged, reopened, and merged again
/// inside one second would otherwise present two real resolutions under one key,
/// and the second would be deduplicated into silence — the mirror of the replay
/// bug, and equally a lie about what happened. `unresolve` increments the
/// counter, and it is the only way back from a terminal status, so it sits
/// between any two resolutions by construction (CAIRN-4767).
///
/// A missing instant falls back to the event's own timestamp: weaker
/// deduplication, but it never suppresses a resolution that has not been
/// announced.
///
/// The two columns are read as ONE row, because together they are a single
/// identity. Read in separate statements, an `unresolve`/re-resolve landing
/// between them yields a torn pair — the old instant beside the new generation —
/// naming a resolution that never existed. A later emit for the genuine
/// resolution then reads a different key and announces it a second time, which
/// is the replay this whole change exists to remove, reintroduced through the
/// back door.
async fn resolution_key(
    db: &LocalDb,
    issue_id: &str,
    final_status: &crate::models::IssueStatus,
    fallback_instant: i64,
) -> String {
    use crate::models::IssueStatus;
    let column = match final_status {
        IssueStatus::Merged => "merged_at",
        IssueStatus::Closed => "closed_at",
        _ => "completed_at",
    };
    match db
        .query_opt(
            format!("SELECT {column}, resolution_generation FROM issues WHERE id = ?1"),
            params![issue_id.to_string()],
            |row| Ok((row.opt_i64(0)?, row.i64(1)?)),
        )
        .await
    {
        Ok(Some((instant, generation))) => {
            format!("{}:{generation}", instant.unwrap_or(fallback_instant))
        }
        // The identity could not be read at all. Rather than invent one out of
        // defaults — which could collide with a real key and silence a genuine
        // resolution — emit a key no well-formed identity can equal, so the
        // guard cannot match and the notice goes out. Delivering twice on a
        // failed read is the recoverable error; never delivering is not.
        Ok(None) | Err(_) => format!("{fallback_instant}:unknown"),
    }
}

pub async fn job_belongs_to_issue(
    db: &LocalDb,
    job_id: &str,
    issue_id: &str,
) -> Result<bool, String> {
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
pub async fn push_to_issue_watchers(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    issue_uri: &str,
    exclude_job: Option<&str>,
    content_ref: &str,
    wake: Wake,
    boundary: Boundary,
    key: &str,
) -> Result<Vec<String>, String> {
    let watchers = crate::wakes::watcher_jobs_for_issue(host.clone(), db, issue_uri).await?;
    let mut pushed = Vec::new();
    for recipient in watchers {
        if Some(recipient.as_str()) == exclude_job {
            continue;
        }
        let (_, effective) = super::push::push(db, &recipient, content_ref, wake, boundary, key)
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
pub async fn create_catchup_push(
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
    let start = match super::push::read_cursor(db, parent_job_id, &child_job_id)
        .await
        .map_err(|e| e.to_string())?
    {
        Some(cursor) => cursor,
        None => (tail - 1).max(0),
    };
    let content_ref = format!("{child_uri}/chat?offset={start}");
    let key = format!("catchup:{child_job_id}");
    super::push::push(
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
/// [`watcher_jobs_for_issue`]: crate::wakes::watcher_jobs_for_issue
pub async fn create_catchup_pushes_for_watchers(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
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
    let watchers = crate::wakes::watcher_jobs_for_issue(host.clone(), db, &issue_uri).await?;
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
pub async fn create_catchup_pushes_for_job(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    child_job_id: &str,
) -> Result<usize, String> {
    let Some(child_uri) = cairn_db::lineage::home_uri_for_job(db, child_job_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    create_catchup_pushes_for_watchers(host, db, &child_uri, None).await
}

async fn issue_id_for_issue_uri(db: &LocalDb, issue_uri: &str) -> Result<Option<String>, String> {
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            Ok(cairn_db::relations::resolve_issue_uri(conn, &issue_uri)
                .await?
                .map(|issue| issue.issue_id))
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
pub const CATCHUP_DIGEST_MESSAGES: usize = 5;
pub const CATCHUP_DIGEST_CHARS: usize = 240;

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
///
/// What the digest may *claim* about those rows is decided by
/// [`DigestAudience`]: a watcher is told what the operator said to a child it
/// watches, and only a job reading its own queue is told it was addressed.
pub async fn catchup_operator_digest(
    db: &LocalDb,
    recipient: &str,
    child_job_id: &str,
) -> Option<String> {
    let anchor = super::push::read_cursor_updated_at(db, recipient, child_job_id)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    let messages = operator_messages_since(db, child_job_id, anchor).await?;
    if messages.is_empty() {
        return None;
    }
    Some(
        OperatorDigest {
            audience: digest_audience(db, recipient, child_job_id).await,
            messages,
        }
        .render(),
    )
}

/// The operator messages recorded against `job_id` that the reader has not
/// caught up on: still pending, or sent at or after `anchor`.
async fn operator_messages_since(
    db: &LocalDb,
    job_id: &str,
    anchor: i64,
) -> Option<Vec<OperatorMessage>> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut messages = Vec::new();
            let mut rows = conn
                .query(
                    "SELECT created_at, content, delivered_at FROM queued_messages
                     WHERE job_id = ?1
                       AND (delivered_at IS NULL OR created_at >= ?2)
                     ORDER BY created_at DESC
                     LIMIT ?3",
                    params![job_id.as_str(), anchor, CATCHUP_DIGEST_MESSAGES as i64],
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
    .ok()
}

/// Resolve who the operator addressed from the durable recipient job and the job
/// the message rows name — never from the text they will be rendered into.
async fn digest_audience(db: &LocalDb, recipient: &str, message_job_id: &str) -> DigestAudience {
    if recipient == message_job_id {
        return DigestAudience::ThisNode;
    }
    DigestAudience::WatchedChild {
        uri: cairn_db::lineage::home_uri_for_job(db, message_job_id)
            .await
            .ok()
            .flatten(),
    }
}

/// Whose messages a digest reports (CAIRN-4608).
///
/// The distinction is the digest's whole point. A watcher's catch-up is a
/// *passive observation* that the operator spoke to a child it watches; a
/// resumed node's own queue is text addressed to that node. Rendered under one
/// wording the two collapse — and after a compaction the prompt they land in is
/// the authoritative context a rebuilt session reads, so the collapse invites a
/// parent to answer messages it never received while hiding that the operator
/// was steering the child by hand. Attribution therefore comes from the durable
/// recipient job and the message rows' own job, resolved in
/// [`digest_audience`].
enum DigestAudience {
    /// The message rows belong to the job being resumed: the operator typed at
    /// this node.
    ///
    /// No creator addresses a node's own chat today —
    /// [`create_catchup_pushes_for_watchers`] excludes jobs on the addressed
    /// node's own issue, and the operator's text to a resumed job reaches it as
    /// its own prompt section through the queue claim rather than through a
    /// digest. This arm is therefore a guard, not a second shipped path: it
    /// keeps the wording a function of resolved provenance instead of a function
    /// of that exclusion holding forever.
    ThisNode,
    /// The message rows belong to a job this recipient merely watches. `uri` is
    /// that child's home address when it resolves; the push header carries the
    /// address either way.
    WatchedChild { uri: Option<String> },
}

/// Operator messages together with the provenance that decides how they may be
/// worded. Rendering goes through this one representation, so an ordinary wake
/// and the seed a rebuilt session composes from that wake's transcript cannot
/// disagree about who was addressed.
struct OperatorDigest {
    audience: DigestAudience,
    messages: Vec<OperatorMessage>,
}

impl OperatorDigest {
    fn render(&self) -> String {
        let count = self.messages.len();
        let noun = if count == 1 { "message" } else { "messages" };
        let lines: Vec<String> = self
            .messages
            .iter()
            .rev()
            .map(|message| {
                let when = cairn_platform::clock::stamp(message.timestamp)
                    .unwrap_or_else(|| "unknown time".to_string());
                let state = if message.read_by_node {
                    String::new()
                } else {
                    " (queued, not yet read by the node)".to_string()
                };
                format!("  • {when}{state} — {}", one_line_excerpt(&message.content))
            })
            .collect();
        let header = match &self.audience {
            DigestAudience::ThisNode => format!(
                "The operator sent {count} {noun} directly to this node that you have not caught up on:"
            ),
            DigestAudience::WatchedChild { uri } => {
                let child = match uri {
                    Some(uri) => format!("{uri}, a child you watch"),
                    None => "a child you watch (its address leads this update)".to_string(),
                };
                format!(
                    "Passive child context — not addressed to you. The operator sent {count} \
                     {noun} to {child}; you have not caught up on them:"
                )
            }
        };
        format!("{header}\n{}", lines.join("\n"))
    }
}

/// One operator message bound for a node.
struct OperatorMessage {
    timestamp: i64,
    content: String,
    /// `false` while the text is still a pending `queued_messages` row the
    /// addressed node has not claimed — sent by the operator, not yet seen by
    /// that node.
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

async fn resolved_issue_confirmation(
    runtime: &AttentionRuntime,
    issue_uri: &str,
) -> Option<String> {
    let project_key = cairn_common::uri::parse_uri(issue_uri)?
        .project_key()?
        .to_string();
    let db = runtime.host.database_for_project(&project_key).await;
    let issue_uri = issue_uri.to_string();
    db.read(|conn| {
        let issue_uri = issue_uri.clone();
        Box::pin(async move {
            let Some(issue) = cairn_db::relations::resolve_issue_uri(conn, &issue_uri).await?
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
/// Direct messages, catch-up digests, terminal resolutions, check verdicts, and
/// posts have bounded, targeted database lookups because their essential detail
/// is not represented by the resource URI alone. Other push kinds deliberately do not
/// render their full resource here: resume prompt assembly holds the launch lock,
/// and making a resource read per pending push made user messages wait tens of
/// seconds. The URI remains the canonical route to the live content through the
/// read tool.
pub async fn render_push_resolved(runtime: &AttentionRuntime, push: &crate::push::Push) -> String {
    let header = format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    );
    if push.key == "build-change" {
        return match runtime
            .host
            .render_content(ContentRequest::BuildChange(&push.content_ref))
            .await
        {
            Some(content) => content,
            _ => header,
        };
    }
    // A schedule wake's content is the reason its recipient wrote for itself.
    // Name that reason in the headline: naming only the referent tells the reader
    // nothing they did not already know.
    if push.key.starts_with("schedule:") {
        let message_id =
            crate::push::latest_push_fingerprint(&runtime.private_db, &push.recipient, &push.key)
                .await
                .ok()
                .flatten()
                .flatten();
        if let Some(message_id) = message_id {
            if let Some(msg) = runtime.host.message(&runtime.private_db, &message_id).await {
                let reason = msg.content.lines().find_map(|line| {
                    let line = line.trim();
                    (!line.is_empty()).then_some(line)
                });
                if let Some(reason) = reason {
                    let mut chars = reason.chars();
                    let label: String = chars.by_ref().take(120).collect();
                    let label = if chars.next().is_some() {
                        format!("{label}…")
                    } else {
                        label
                    };
                    let schedule_header = format!(
                        "Attention update ({}): scheduled wake — {}",
                        push.wake.as_str(),
                        label
                    );
                    let body = crate::message_render::render_direct_message(&msg);
                    return format!("{schedule_header}\n\n{body}");
                }
            }
        }
        return header;
    }
    if push.key.starts_with("resolved:") {
        return match resolved_issue_confirmation(runtime, &push.content_ref).await {
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
        if let Some(body) = runtime
            .host
            .render_content(ContentRequest::Checks(&push.recipient))
            .await
        {
            return format!("{header}\n\n{body}");
        }
    }

    // A `child-stalled:` push exists to say that a child stopped being able to
    // work, and the whole of what the parent needs is which node, why, and how
    // many times. A bare node URI carries none of that, and worse, it looks like
    // an ordinary invitation to go and read a running node (CAIRN-4609).
    if push
        .key
        .starts_with(&format!("{}:", crate::push::CHILD_STALL_PUSH_PREFIX))
    {
        if let Some(body) = runtime
            .host
            .render_content(ContentRequest::ChildStall {
                recipient: &push.recipient,
                reference: &push.content_ref,
            })
            .await
        {
            return format!("{header}\n\n{body}");
        }
    }

    // A `direct:` push carries frozen message content, not an idempotent
    // resolvable referent. Resolve it from the durable `messages` row by the
    // message id in the key (`direct:{message_id}`) rather than from
    // `content_ref` (which is the conversation surface the wake card links to).
    // Falls back to the header line if the row is missing (CAIRN-1900).
    if let Some(message_id) = crate::push::direct_message_id_from_key(&push.key) {
        return match runtime.host.message(&runtime.private_db, message_id).await {
            Some(msg) => {
                let body = crate::message_render::render_direct_message(&msg);
                format!("{header}\n\n{body}")
            }
            _ => header,
        };
    }
    // A `catchup:` push carries the operator-message digest without rendering the
    // potentially enormous chat window. The URI remains available for follow-up.
    let digest = match push.key.strip_prefix("catchup:") {
        Some(child_job_id) => {
            let db = runtime
                .host
                .database_for_job(child_job_id)
                .await
                .unwrap_or_else(|_| runtime.private_db.clone());
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

    // A post push's referent is a short piece of writing whose whole purpose is
    // to be classified — worth reading further, or not — and a subscriber cannot
    // classify a URI. The post (or the comment) is carried inline from the
    // workspace-local corpus, two indexed lookups, the same cheap-and-targeted
    // bargain the direct-message and check-verdict branches above make.
    if crate::wakes::post_push_source(prefix).is_some() {
        if let Some(body) = runtime
            .host
            .render_content(ContentRequest::Post(push))
            .await
        {
            // The headline is kept in front of the content because it carries
            // the one thing the post itself cannot: why this recipient is being
            // told. Being referenced by a post is not the same event as one
            // landing in a feed you elected to watch.
            let (_, headline) = super::push::push_kind_headline(prefix);
            return format!("{header}\n\n{headline}.\n\n{body}");
        }
    }

    let (_, headline) = super::push::push_kind_headline(prefix);
    format!(
        "{header}\n\n{headline}. Read {} for the current content.",
        push.content_ref
    )
}

/// Render several pushes concurrently into one compact block, or `None` when the
/// slice is empty so callers can fold it into an optional prompt section.
pub async fn render_pushes_resolved(
    runtime: &AttentionRuntime,
    pushes: &[crate::push::Push],
) -> Option<String> {
    if pushes.is_empty() {
        return None;
    }
    use futures_util::{stream, StreamExt};

    let mut blocks = stream::iter(pushes.iter().enumerate())
        .map(|(index, push)| async move { (index, render_push_resolved(runtime, push).await) })
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
