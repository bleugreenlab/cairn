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
//!    details are retained for direct messages, catch-up digests and the
//!    child's own chat line, review titles and summaries, terminal resolutions,
//!    check verdicts, and posts — the referents whose point is lost when only
//!    their address arrives. Each detail is a few indexed statements bounded by
//!    what it shows, and any miss degrades to the bare pointer rather than an
//!    error; full resources are read by the agent only when needed.

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
    if !matches!(event.fact, AttentionFact::Resolved { .. }) {
        return;
    }
    let runtime = runtime.clone();
    let event = event.clone();
    if let Err(error) =
        run_db_blocking(move || async move { create_resolved_push_async(&runtime, &event).await })
    {
        log::warn!("resolved push creation failed: {error}");
    }
}

pub(crate) async fn create_resolved_push_async(
    runtime: &AttentionRuntime,
    event: &AttentionEvent,
) -> Result<usize, String> {
    let final_status = match &event.fact {
        AttentionFact::Resolved { final_status } => final_status.clone(),
        _ => return Ok(0),
    };
    let db = runtime.host.database_for_issue(&event.issue_id).await?;
    let Some(identity) =
        resolution_key(&db, &event.issue_id, &final_status, event.updated_at).await?
    else {
        return Ok(0);
    };
    let fingerprint = format!("status:{final_status}:{identity}");
    let key = format!("resolved:{}", event.issue_uri);
    // A failed resolution wakes every watcher; any other wakes only the
    // coordinator and rides along for the rest.
    let watcher_level = if final_status == crate::models::IssueStatus::Failed {
        Wake::Wake
    } else {
        Wake::Passive
    };
    let mut audience = crate::wakes::FactAudience::new("resolved", watcher_level, Wake::Wake);
    audience.source = Some(crate::wakes::SourceRef::issue(&event.issue_uri));
    audience.exclude.subject_issue_jobs = true;
    let push = AudiencePush {
        // The resolution's own identity (instant and generation): reopening
        // and resolving again is a new fact, a replay of this one is not.
        fact_key: format!("{key}#{fingerprint}"),
        content: PushContent::Fixed(event.issue_uri.clone()),
        boundary: Boundary::Event,
        key,
        fingerprint: Some(fingerprint.clone()),
        guard: PushGuard::Resolution {
            issue_id: event.issue_id.clone(),
            status: final_status.to_string(),
            fingerprint,
            fallback_instant: event.updated_at,
        },
    };
    let deliveries = push_to_audience(runtime.host.clone(), &db, &audience, &push).await?;
    let mut created = 0;
    for delivery in &deliveries {
        // Chapter bookkeeping is idempotent and also runs on replay: a crash
        // after push commit but before this mark is repaired by the next boot.
        runtime
            .host
            .mark_thread_chapter(
                &db,
                &delivery.job_id,
                &event.issue_id,
                &event.issue_uri,
                &final_status.to_string(),
            )
            .await;
        if !delivery.fresh {
            continue;
        }
        created += 1;
        if delivery.wake.wakes_idle() {
            if let Err(error) = runtime
                .host
                .nudge(&delivery.job_id, cairn_domain::DeliveryUrgency::Steer)
            {
                log::warn!("resolved push wake for {} failed: {error}", delivery.job_id);
            }
        }
    }
    if created > 0 {
        runtime.host.emit_change("attention_pushes", None);
    }
    Ok(created)
}

/// What one push carries to each recipient it reaches.
#[derive(Clone, Debug)]
pub enum PushContent {
    /// The same referent for everyone.
    Fixed(String),
    /// A catch-up window over `child_uri`'s chat, from each recipient's own
    /// read cursor for `child_job_id`, or one turn of lead-in before `tail`.
    Catchup {
        child_uri: String,
        child_job_id: String,
        tail: i64,
    },
}

/// A condition the push re-checks inside its own transaction, so a stale
/// caller can never authorize it.
#[derive(Clone, Debug)]
pub enum PushGuard {
    None,
    /// A review: only while its issue is unresolved.
    UnresolvedIssue(String),
    /// A resolution notice: only while the issue still holds this resolution.
    Resolution {
        issue_id: String,
        status: String,
        fingerprint: String,
        fallback_instant: i64,
    },
}

/// One issue-sourced push owed to everyone the resolver finds.
#[derive(Clone, Debug)]
pub struct AudiencePush {
    /// The receipt key. It must tell a genuine recurrence from a retry: an
    /// ask id, a fingerprint naming the reviewed head or the episode, a
    /// resolution identity, a catch-up occurrence.
    pub fact_key: String,
    pub content: PushContent,
    pub boundary: Boundary,
    /// The push key, which supersedes an undelivered push of the same key.
    pub key: String,
    pub fingerprint: Option<String>,
    pub guard: PushGuard,
}

/// One recipient a push reached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PushDelivery {
    pub recipient: crate::wakes::StableRecipient,
    /// The session job (or job) that holds the push.
    pub job_id: String,
    pub wake: Wake,
    /// `false` when the receipt was already delivered (a retry or a replay):
    /// nothing new was pushed and nobody should be woken for it.
    pub fresh: bool,
}

/// Push `push` to every recipient `audience` resolves to, writing each
/// recipient's receipt in the same transaction as its push: for a push,
/// receipt creation is the delivery. A receipt already consumed is never
/// pushed again; a closed or fenced recipient's receipt is `dropped`.
/// Callers nudge the fresh deliveries that wake.
pub async fn push_to_audience(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    audience: &crate::wakes::FactAudience,
    push: &AudiencePush,
) -> Result<Vec<PushDelivery>, String> {
    let key = push.key.clone();
    let audience = audience.clone();
    let push = push.clone();
    db.write(move |conn| {
        let host = host.clone();
        let audience = audience.clone();
        let push = push.clone();
        Box::pin(async move { push_to_audience_conn(host, conn, &audience, &push).await })
    })
    .await
    .map_err(|error| format!("Failed to push {key}: {error}"))
}

async fn push_to_audience_conn(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    conn: &cairn_db::storage::TrackedConnection,
    audience: &crate::wakes::FactAudience,
    push: &AudiencePush,
) -> crate::storage::DbResult<Vec<PushDelivery>> {
    use crate::wakes::{receipt_state, NewReceipt};
    if !push_guard_holds_conn(conn, &push.guard).await? {
        return Ok(Vec::new());
    }
    let candidates = crate::wakes::resolve_recipients_conn(host.clone(), conn, audience)
        .await?
        .into_candidates();
    let mut deliveries = Vec::new();
    for candidate in candidates {
        if let Some(existing) =
            crate::wakes::receipt_conn(conn, &push.fact_key, &candidate.recipient).await?
        {
            if existing.state != receipt_state::QUEUED {
                if let (true, Some(job_id)) = (
                    existing.state == receipt_state::DELIVERED,
                    existing.delivered_job_id,
                ) {
                    deliveries.push(PushDelivery {
                        recipient: candidate.recipient.clone(),
                        job_id,
                        wake: existing.level,
                        fresh: false,
                    });
                }
                continue;
            }
        }
        let new = NewReceipt {
            fact_key: &push.fact_key,
            candidate: &candidate,
            project_id: audience.project_id.as_deref(),
            fact_row_id: None,
            fact_json: None,
        };
        let write = crate::wakes::upsert_receipt_conn(conn, &new).await?;
        let receipt = write.receipt().clone();
        let job_id = match crate::wakes::deliverable_job_conn(
            host.clone(),
            conn,
            &candidate.recipient,
            true,
        )
        .await?
        {
            Ok(job_id) => job_id,
            Err(why) => {
                crate::wakes::close_receipts_conn(
                    conn,
                    &[receipt.id],
                    receipt_state::DROPPED,
                    why.describe(),
                )
                .await?;
                continue;
            }
        };
        let content_ref = push_content_ref_conn(conn, &push.content, &job_id).await?;
        let pushed = super::push::push_fingerprinted_conn(
            conn,
            &job_id,
            &content_ref,
            receipt.level,
            push.boundary,
            &push.key,
            push.fingerprint.as_deref(),
            push.fingerprint.is_some(),
        )
        .await?;
        crate::wakes::settle_receipts_conn(
            conn,
            std::slice::from_ref(&receipt.id),
            &job_id,
            None,
            pushed.as_ref().map(|(id, _)| id.as_str()),
        )
        .await?;
        deliveries.push(PushDelivery {
            recipient: candidate.recipient,
            job_id,
            wake: receipt.level,
            // An identical fingerprint already pushed (before receipts existed)
            // is the same fact: settled, but not pushed or woken again.
            fresh: pushed.is_some(),
        });
    }
    Ok(deliveries)
}

async fn push_guard_holds_conn(
    conn: &cairn_db::storage::TrackedConnection,
    guard: &PushGuard,
) -> crate::storage::DbResult<bool> {
    match guard {
        PushGuard::None => Ok(true),
        PushGuard::UnresolvedIssue(issue_id) => {
            Ok(super::push::review_issue_eligible_conn(conn, issue_id).await? > 0)
        }
        PushGuard::Resolution {
            issue_id,
            status,
            fingerprint,
            fallback_instant,
        } => {
            // Reopening and replay serialize with insertion, including
            // delivered history. A stale event cannot push or mark a chapter.
            let mut rows = conn
                .query(
                    "SELECT status,
                        CASE status WHEN 'merged' THEN merged_at WHEN 'closed' THEN closed_at
                            ELSE completed_at END,
                        resolution_generation, quiet_failure_generation
                     FROM issues WHERE id=?1",
                    (issue_id.as_str(),),
                )
                .await?;
            let Some(row) = rows.next().await? else {
                return Ok(false);
            };
            let generation = row.i64(2)?;
            if row.text(0)? != *status
                || (status == "failed" && row.opt_i64(3)? == Some(generation))
            {
                return Ok(false);
            }
            let current = format!(
                "status:{status}:{}:{generation}",
                row.opt_i64(1)?.unwrap_or(*fallback_instant)
            );
            Ok(current == *fingerprint)
        }
    }
}

async fn push_content_ref_conn(
    conn: &cairn_db::storage::TrackedConnection,
    content: &PushContent,
    job_id: &str,
) -> crate::storage::DbResult<String> {
    match content {
        PushContent::Fixed(content_ref) => Ok(content_ref.clone()),
        PushContent::Catchup {
            child_uri,
            child_job_id,
            tail,
        } => {
            let mut rows = conn
                .query(
                    "SELECT position FROM attention_read_cursors
                     WHERE recipient=?1 AND source=?2 LIMIT 1",
                    params![job_id, child_job_id.as_str()],
                )
                .await?;
            let start = match rows.next().await? {
                Some(row) => row.i64(0)?,
                None => (tail - 1).max(0),
            };
            Ok(format!("{child_uri}/chat?offset={start}"))
        }
    }
}

/// Recover failed resolutions committed before their attention emit. Existing
/// pending delivery owns resuming recipients; this only reconstructs missing
/// pushes through the normal creator and its current watcher/mute policy.
pub async fn rearm_failed_resolutions(
    runtime: &AttentionRuntime,
    db: &LocalDb,
) -> Result<usize, String> {
    let candidates = db
        .read(|conn| {
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        "SELECT i.id, p.key, i.number, COALESCE(i.completed_at,i.updated_at)
             FROM issues i JOIN projects p ON p.id=i.project_id
             WHERE i.status='failed'
               AND (i.quiet_failure_generation IS NULL
                    OR i.quiet_failure_generation != i.resolution_generation)
             ORDER BY i.id",
                        (),
                    )
                    .await?;
                let mut candidates = Vec::new();
                while let Some(row) = rows.next().await? {
                    candidates.push((
                        row.text(0)?,
                        cairn_uri::build_issue_uri(&row.text(1)?, row.i64(2)? as i32),
                        row.i64(3)?,
                    ));
                }
                Ok(candidates)
            })
        })
        .await
        .map_err(|e| e.to_string())?;
    let mut created = 0;
    for (issue_id, issue_uri, updated_at) in candidates {
        let event = AttentionEvent {
            issue_id,
            issue_uri,
            updated_at,
            attention: crate::models::IssueAttention::None,
            status: crate::models::IssueStatus::Failed,
            route_provenance: None,
            fact: AttentionFact::Resolved {
                final_status: crate::models::IssueStatus::Failed,
            },
        };
        match create_resolved_push_async(runtime, &event).await {
            Ok(count) => created += count,
            Err(error) => log::warn!("failed resolution startup re-arm: {error}"),
        }
    }
    Ok(created)
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
/// by the resolving transition and cleared on explicit or implicit reopen, so it
/// is unmoved by any number of recomputes. That is what stops the replay.
///
/// The **generation** is what makes a genuine re-resolution distinguishable. The
/// instants are whole seconds, so an issue merged, reopened, and merged again
/// inside one second would otherwise present two real resolutions under one key,
/// and the second would be deduplicated into silence — the mirror of the replay
/// bug, and equally a lie about what happened. Explicit `unresolve` increments
/// the counter; the canonical issue projection also increments it when retry
/// facts reopen failed work, without double-counting explicit reopen.
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
) -> Result<Option<String>, String> {
    use crate::models::IssueStatus;
    let column = match final_status {
        IssueStatus::Merged => "merged_at",
        IssueStatus::Closed => "closed_at",
        _ => "completed_at",
    };
    db.query_opt(
        format!("SELECT {column}, resolution_generation FROM issues WHERE id = ?1"),
        params![issue_id.to_string()],
        |row| Ok((row.opt_i64(0)?, row.i64(1)?)),
    )
    .await
    .map(|identity| {
        identity.map(|(instant, generation)| {
            format!("{}:{generation}", instant.unwrap_or(fallback_instant))
        })
    })
    .map_err(|error| error.to_string())
}

/// Push an ask (a question or a permission) to everyone the resolver finds
/// for `issue_uri`, excluding the producing node, which is self-suspended on
/// its own ask. The ask's own URI is its fact key: one ask is one fact.
///
/// Returns the jobs a fresh push should rouse, so a `wake`/`interrupt` caller
/// can wake each through `delivery::nudge_job_for_urgency` (CAIRN-1889). A
/// recipient that muted this source holds a `Passive` ride-along row and is
/// not handed back (CAIRN-1900).
#[expect(
    clippy::too_many_arguments,
    reason = "watcher routing and push delivery fields are independent parts of this boundary"
)]
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
    let fact_kind = key.split_once(':').map_or(key, |(prefix, _)| prefix);
    let mut audience = crate::wakes::FactAudience::new(fact_kind, wake, wake);
    audience.source = Some(crate::wakes::SourceRef::issue(issue_uri));
    audience.exclude.jobs = exclude_job.map(str::to_owned).into_iter().collect();
    let push = AudiencePush {
        fact_key: key.to_string(),
        content: PushContent::Fixed(content_ref.to_string()),
        boundary,
        key: key.to_string(),
        fingerprint: None,
        guard: PushGuard::None,
    };
    Ok(push_to_audience(host, db, &audience, &push)
        .await?
        .into_iter()
        .filter(|delivery| delivery.fresh && delivery.wake.wakes_idle())
        .map(|delivery| delivery.job_id)
        .collect())
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

/// Create the passive catch-up push for everyone the resolver finds for the
/// issue the addressed node belongs to (CAIRN-3342), for a message the
/// **operator** sent that node: the composer's queued text, an answer to a
/// question, or a direct message with no sending run. Agent-to-agent traffic
/// inside a child never comes through here; [`create_catchup_push`] is the
/// per-recipient primitive underneath it.
///
/// The recipient set is derived, never materialized: the resolver finds the
/// issue's current coordinator on every call, plus explicit subscriptions and
/// the issue's camp. A new coordinator execution therefore inherits its
/// children's operator traffic with no hand-off step, and a superseded one
/// stops receiving it.
///
/// `occurrence` names the operator message, so each message is its own fact
/// while a retry of one is not. Jobs on the addressed node's own issue are
/// excluded, so a node never receives catch-up for its own chat. Catch-up is
/// always passive: a muted watcher's ride-along is what a mute would leave it.
pub async fn create_catchup_pushes_for_watchers(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    child_uri: &str,
    occurrence: &str,
) -> Result<usize, String> {
    let Some(parsed) = cairn_uri::parse_uri(child_uri) else {
        return Ok(0);
    };
    let (Some(project_key), Some(number)) = (
        parsed.project().map(cairn_uri::canonical_project),
        parsed.issue_number(),
    ) else {
        return Ok(0);
    };
    let Some(child_job_id) = job_id_for_child_uri(db, child_uri).await else {
        // The URI did not resolve to an agent job (a stale or not-yet-persisted
        // node/task, or a non-node/task URI); skip rather than mis-scope the
        // cursor.
        log::warn!("catch-up push: no job for {child_uri}");
        return Ok(0);
    };
    let issue_uri = cairn_uri::build_issue_uri(&project_key, number);
    let tail = count_job_chat_turns(db, &child_job_id).await;
    let key = format!("catchup:{child_job_id}");
    let mut audience = crate::wakes::FactAudience::new(
        crate::wakes::FACT_KIND_MESSAGE,
        Wake::Passive,
        Wake::Passive,
    );
    audience.source = Some(crate::wakes::SourceRef::issue(&issue_uri));
    audience.exclude.subject_issue_jobs = true;
    let push = AudiencePush {
        fact_key: format!("{key}#{occurrence}"),
        content: PushContent::Catchup {
            child_uri: child_uri.to_string(),
            child_job_id,
            tail,
        },
        boundary: Boundary::Event,
        key,
        fingerprint: None,
        guard: PushGuard::None,
    };
    Ok(push_to_audience(host, db, &audience, &push)
        .await?
        .into_iter()
        .filter(|delivery| delivery.fresh)
        .count())
}

/// The node/task URI of `job_id`, then [`create_catchup_pushes_for_watchers`] on
/// it. The entry point for callers holding a job rather than a URI — the desktop
/// composer, whose text reaches a job id and never an addressed URI.
pub async fn create_catchup_pushes_for_job(
    host: std::sync::Arc<dyn crate::runtime::RecipientHost>,
    db: &LocalDb,
    child_job_id: &str,
    occurrence: &str,
) -> Result<usize, String> {
    let Some(child_uri) = cairn_db::lineage::home_uri_for_job(db, child_job_id)
        .await
        .map_err(|error| error.to_string())?
    else {
        return Ok(0);
    };
    create_catchup_pushes_for_watchers(host, db, &child_uri, occurrence).await
}

/// Resolve the agent job whose `{node|task}/chat` a catch-up push renders, from
/// the addressed child URI — the same job the chat resource resolves. A node URI
/// maps to its top-level job (issue + execution seq + node `uri_segment`); a task
/// URI maps to the addressed sub-task job (the task `uri_segment` under that node
/// job), so a user message directed at a sub-task still scopes catch-up to the
/// task's own chat. `None` for any other URI or an unresolved node/task.
async fn job_id_for_child_uri(db: &LocalDb, child_uri: &str) -> Option<String> {
    let (project, number, exec_seq, node_id, task_name) = match cairn_uri::parse_uri(child_uri)? {
        cairn_uri::CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => (project, number, exec_seq, node_id, None),
        cairn_uri::CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => (project, number, exec_seq, node_id, Some(task_name)),
        _ => return None,
    };
    db.read(|conn| {
        let project = project.clone();
        let node_id = node_id.clone();
        let task_name = task_name.clone();
        Box::pin(async move {
            job_id_for_node_coordinates(
                conn,
                &project,
                number,
                exec_seq,
                &node_id,
                task_name.as_deref(),
            )
            .await
        })
    })
    .await
    .ok()
    .flatten()
}

/// The agent job a node or task address names: the top-level node job (issue +
/// execution seq + node `uri_segment`), or, given a `task_name`, the sub-task job
/// with that `uri_segment` under it. `None` when either does not resolve. The one
/// resolver behind every node/task URI attention reads through.
async fn job_id_for_node_coordinates(
    conn: &cairn_db::storage::TrackedConnection,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> crate::storage::DbResult<Option<String>> {
    let project = cairn_uri::canonical_project(project);
    let mut rows = conn
        .query(
            "SELECT j.id FROM jobs j
             JOIN executions e ON j.execution_id = e.id
             JOIN issues i ON j.issue_id = i.id
             JOIN projects p ON i.project_id = p.id
             WHERE p.key=?1 AND i.number=?2 AND e.seq=?3
               AND j.uri_segment=?4 AND j.parent_job_id IS NULL
             LIMIT 1",
            params![project.as_str(), number as i64, exec_seq as i64, node_id],
        )
        .await?;
    let node_job_id = match rows.next().await? {
        Some(row) => row.text(0)?,
        None => return Ok(None),
    };
    let Some(task_name) = task_name else {
        return Ok(Some(node_job_id));
    };
    let mut task_rows = conn
        .query(
            "SELECT id FROM jobs WHERE parent_job_id=?1 AND uri_segment=?2 LIMIT 1",
            params![node_job_id.as_str(), task_name],
        )
        .await?;
    match task_rows.next().await? {
        Some(row) => Ok(Some(row.text(0)?)),
        None => Ok(None),
    }
}

/// Distinct turns recorded across one job's runs — the job-scoped chat tail that
/// `{node}/chat` renders. Catch-up's window start when the parent has no prior
/// read cursor.
async fn count_job_chat_turns(db: &LocalDb, job_id: &str) -> i64 {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move { super::push::count_job_chat_turns(conn, &job_id).await })
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

/// Collapse a message to one line of at most [`CATCHUP_DIGEST_CHARS`], plus an
/// ellipsis when it was cut.
///
/// A cut lands where the reader can stop: after the last sentence end inside the
/// bound, else at the last word break, provided either keeps at least half the
/// bound; only a single unbroken run is cut mid-token.
pub fn one_line_excerpt(content: &str) -> String {
    let flattened = content.split_whitespace().collect::<Vec<_>>().join(" ");
    if flattened.chars().count() <= CATCHUP_DIGEST_CHARS {
        return flattened;
    }
    // One character past the bound, so a sentence or word that ends exactly at
    // the bound is still seen to end there.
    let probe: String = flattened.chars().take(CATCHUP_DIGEST_CHARS + 1).collect();
    let byte_at = |chars: usize| {
        probe
            .char_indices()
            .nth(chars)
            .map_or(probe.len(), |(index, _)| index)
    };
    let bound = byte_at(CATCHUP_DIGEST_CHARS);
    let floor = byte_at(CATCHUP_DIGEST_CHARS / 2);
    let sentence = [". ", "! ", "? "]
        .iter()
        .filter_map(|end| {
            probe
                .rmatch_indices(end)
                .map(|(index, _)| index + 1)
                .find(|cut| *cut <= bound)
        })
        .max()
        .filter(|cut| *cut >= floor);
    let word = || {
        probe
            .rmatch_indices(' ')
            .map(|(index, _)| index)
            .find(|cut| *cut <= bound)
            .filter(|cut| *cut >= floor)
    };
    let cut = sentence.or_else(word).unwrap_or(bound);
    format!("{}…", probe[..cut].trim_end())
}

/// How many of a catch-up window's newest turns the chat line searches for
/// assistant text before settling for the turn count alone.
const CATCHUP_SCAN_TURNS: i64 = 8;

/// Prose candidates one turn offers the chat line, newest first. A candidate is
/// passed over only when it is a sub-agent's reply or an archived row that
/// decodes empty, so a handful is plenty.
const CATCHUP_TURN_CANDIDATES: i64 = 4;

/// A job's newest `?2` turns the chat renders, newest first: the same turns
/// [`count_job_chat_turns`] counts, so the window walked is the window counted.
/// `idx_turns_job_id` holds a job's turns in insertion order, so this walks the
/// index backwards with no sort over the job's history.
///
/// [`count_job_chat_turns`]: super::push::count_job_chat_turns
pub(crate) const WINDOW_TURNS_SQL: &str = concat!(
    "SELECT id, run_id, created_at FROM turns WHERE ",
    super::push::job_chat_turns_where!(),
    " ORDER BY rowid DESC LIMIT ?2"
);

/// One turn's newest top-level-assistant prose candidates as keys: run `?1`,
/// from the turn's start `?2`, turn `?3`, at most `?4`. Through the prose index
/// this is a range of index entries that never opens a row.
pub(crate) fn prose_candidates_sql(index: &str) -> String {
    use cairn_db::storage::events::columns::PROSE_SHAPE;
    format!(
        "SELECT e.id, e.created_at, e.rowid FROM events e INDEXED BY {index}
         WHERE e.run_id = ?1 AND e.event_type = 'assistant' AND e.created_at >= ?2
           AND e.turn_id = ?3 AND {PROSE_SHAPE}
         ORDER BY e.created_at DESC, e.id DESC LIMIT ?4"
    )
}

/// One line naming what a child's own chat has accrued since the reader's
/// catch-up cursor: the node, the number of new turns, and the opening of the
/// newest assistant text among them, then the window's address.
///
/// The window starts at the `offset` the push's `content_ref` carries, which is
/// the reader's cursor (or one lead-in turn on a first catch-up), so the count
/// and the address always describe the same turns. Only assistant prose is
/// quoted: a `user` event is a role, not an author, and quoting it would echo a
/// launch prompt back as news (CAIRN-3408). `None` when the window is empty or
/// anything fails to read, so the caller keeps the pointer.
async fn catchup_chat_line(db: &LocalDb, content_ref: &str, child_job_id: &str) -> Option<String> {
    use cairn_uri::CairnResource;
    let (address, query) = content_ref.split_once('?')?;
    let offset: i64 = query
        .split('&')
        .find_map(|pair| pair.strip_prefix("offset="))?
        .parse()
        .ok()?;
    let node = match cairn_uri::parse_uri(address)? {
        CairnResource::NodeChat { node_id, .. } => node_id,
        CairnResource::TaskChat {
            node_id, task_name, ..
        } => format!("{node_id}/task/{task_name}"),
        _ => return None,
    };
    let job_id = child_job_id.to_string();
    let (new_turns, candidates) = db
        .read(|conn| {
            let job_id = job_id.clone();
            Box::pin(async move {
                let new_turns = super::push::count_job_chat_turns(conn, &job_id).await? - offset;
                if new_turns <= 0 {
                    return Ok::<_, crate::storage::DbError>((new_turns, Vec::new()));
                }
                let scan = new_turns.min(CATCHUP_SCAN_TURNS);
                let candidates = newest_assistant_prose(conn, &job_id, scan).await?;
                Ok((new_turns, candidates))
            })
        })
        .await
        .map_err(|error| log::debug!("catch-up chat line for {child_job_id}: {error}"))
        .ok()?;
    if new_turns <= 0 {
        return None;
    }
    let text = cairn_db::storage::events::reconstruct_events(db, candidates)
        .await
        .into_iter()
        .find_map(|event| assistant_text(&event.data));
    let noun = if new_turns == 1 { "turn" } else { "turns" };
    Some(match text {
        Some(text) => format!(
            "{node} · {new_turns} new {noun} · \"{}\" Read {content_ref}",
            one_line_excerpt(&text)
        ),
        None => format!("{node} · {new_turns} new {noun}. Read {content_ref}"),
    })
}

/// The newest top-level assistant prose in a job's newest `scan` turns, as stored
/// rows for [`reconstruct_events`] to decode, newest first.
///
/// Every step is bounded by the window, never by the job's history. The turns
/// come off `idx_turns_job_id` newest first, which needs no sort because that
/// index holds a job's turns in insertion order. Each turn's candidates are a
/// range of the prose index from the turn's own start, read as keys alone. A
/// row's body is read only for a candidate. The walk stops at the first plain
/// (`full`) row, which the index predicate guarantees holds text; archived rows
/// met before it are kept for the decoder to settle.
///
/// [`reconstruct_events`]: cairn_db::storage::events::reconstruct_events
async fn newest_assistant_prose(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
    scan: i64,
) -> crate::storage::DbResult<Vec<crate::models::Event>> {
    use cairn_db::storage::events::columns::{event_from_row, EVENT_COLUMNS};
    let mut turns = Vec::new();
    let mut rows = conn.query(WINDOW_TURNS_SQL, params![job_id, scan]).await?;
    while let Some(row) = rows.next().await? {
        turns.push((row.text(0)?, row.opt_text(1)?, row.i64(2)?));
    }
    drop(rows);
    let candidates_sql = prose_candidates_sql(prose_walk_index(conn).await?);
    let row_sql = format!("SELECT {EVENT_COLUMNS} FROM events WHERE id = ?1");
    let mut found = Vec::new();
    for (turn_id, run_id, started) in turns {
        // A run-less turn holds only a session boundary: one of the window's
        // turns, but never assistant prose.
        let Some(run_id) = run_id else {
            continue;
        };
        let mut keys = Vec::new();
        let mut rows = conn
            .query(
                &candidates_sql,
                params![
                    run_id.as_str(),
                    started,
                    turn_id.as_str(),
                    CATCHUP_TURN_CANDIDATES
                ],
            )
            .await?;
        while let Some(row) = rows.next().await? {
            keys.push((row.i64(1)?, row.i64(2)?, row.text(0)?));
        }
        drop(rows);
        // The index breaks a same-second tie by id, a UUID; the chat breaks it by
        // insertion order.
        keys.sort_by_key(|(created_at, rowid, _)| std::cmp::Reverse((*created_at, *rowid)));
        for (_, _, id) in keys {
            let mut rows = conn.query(&row_sql, params![id.as_str()]).await?;
            let Some(row) = rows.next().await? else {
                continue;
            };
            let event = event_from_row(&row)?;
            if event.parent_tool_use_id.is_some() {
                continue;
            }
            let plain = matches!(event.storage_mode.as_deref(), None | Some("full"));
            found.push(event);
            if plain {
                return Ok(found);
            }
        }
    }
    Ok(found)
}

/// The index a turn's prose candidates are read through. `idx_events_home_prose`
/// belongs to the private lineage, and `INDEXED BY` a missing index is an error
/// rather than a hint, so a team database reads the turn's own events instead:
/// still bounded by the turn, at the cost of opening its rows.
async fn prose_walk_index(
    conn: &cairn_db::storage::TrackedConnection,
) -> crate::storage::DbResult<&'static str> {
    let mut rows = conn
        .query(
            "SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'idx_events_home_prose'",
            (),
        )
        .await?;
    Ok(if rows.next().await?.is_some() {
        "idx_events_home_prose"
    } else {
        "idx_events_turn_id"
    })
}

/// The model's text in a stored assistant event: the canonical
/// `{"eventType":"assistant","content":"…"}` shape. Tool calls and thinking
/// blocks carry no `content` string and yield nothing.
fn assistant_text(data: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(data).ok()?;
    let content = value.get("content")?.as_str()?;
    (!content.trim().is_empty()).then(|| content.to_string())
}

/// What a review push asks the reader to look at: the artifact's preview from
/// the references resolver, which reads its own `title` and optional `summary`
/// as bounded lines and never the body, then the address. The resolver reads a
/// node's generic `.../artifact` alias as its PR, not as "the newest artifact of
/// any name", which could be a later working document such as a `plan`
/// (CAIRN-1953). `None` for an address that is not a node or task artifact, or
/// whose artifact does not resolve, parse, or carry a title.
async fn review_artifact_line(runtime: &AttentionRuntime, content_ref: &str) -> Option<String> {
    use cairn_common::references::ReferenceEntry;
    use cairn_uri::CairnResource;
    if !matches!(
        cairn_uri::parse_uri(content_ref)?,
        CairnResource::NodeArtifact { .. } | CairnResource::TaskArtifact { .. }
    ) {
        return None;
    }
    let ReferenceEntry::Preview { title, line, .. } = runtime.host.reference(content_ref).await?
    else {
        return None;
    };
    let summary = line.map(|line| format!("{line}\n")).unwrap_or_default();
    Some(format!(
        "Work product ready for review: {title}\n{summary}Read {content_ref} for the current content."
    ))
}

/// A direct message as its recipient reads it: the header, the content and the
/// reply hint, then the references block for the URIs the content cites, so
/// the sender never has to paste what a cited URI holds.
async fn direct_message_body(runtime: &AttentionRuntime, msg: &cairn_domain::Message) -> String {
    let body = crate::message_render::render_direct_message(msg);
    let cited = cairn_common::references::without_references_block(&msg.content);
    match runtime.host.references_block(cited).await {
        Some(block) => format!("{body}\n\n{block}"),
        None => body,
    }
}

/// A persisted own failure, rather than a descendant's inherited failed projection.
#[derive(Debug, Clone)]
pub struct FailureEvidence {
    pub job_id: Option<String>,
    pub node_uri: String,
    pub reason: String,
}

/// Read the latest work attempt only. A failed memory review or an older failed
/// attempt must not stand in for a successful retry. Synthetic rejection turns
/// have no run id, so their detail lives on the job's latest run.
pub async fn job_failure_reason_conn(
    conn: &cairn_db::storage::TrackedConnection,
    job_id: &str,
) -> crate::storage::DbResult<Option<String>> {
    let mut rows = conn
        .query(
            "SELECT t.state, COALESCE(NULLIF(t.failure_reason,''), NULLIF(r.error_message,''), t.end_reason, r.exit_reason),
                r.status, r.exit_reason, j.status, j.return_abandoned_at
         FROM jobs j
         LEFT JOIN turns t ON t.id=(
             SELECT id FROM turns WHERE job_id=j.id AND start_reason != 'memory_review'
             ORDER BY created_at DESC, sequence DESC LIMIT 1)
         LEFT JOIN runs r ON r.id=COALESCE(t.run_id, (
             SELECT id FROM runs WHERE job_id=j.id ORDER BY created_at DESC, rowid DESC LIMIT 1))
         WHERE j.id=?1",
            (job_id,),
        )
        .await?;
    let Some(row) = rows.next().await? else {
        return Ok(None);
    };
    let state = row.opt_text(0)?;
    // The workflow projection has already evaluated the output contract. Do not
    // rederive it here: Blocked missing-output work remains nonterminal and silent.
    if state.as_deref() == Some("complete") && row.text(4)? == "failed" && row.opt_i64(5)?.is_some()
    {
        return Ok(Some(
            "Required output was not produced after the return was abandoned".into(),
        ));
    }
    let own_failure = state.as_deref() == Some("failed")
        || (state.is_none()
            && matches!(row.opt_text(2)?.as_deref(), Some("failed" | "crashed"))
            && row.opt_text(3)?.as_deref() != Some("user_stop"));
    if !own_failure {
        return Ok(None);
    }
    let reason = row
        .opt_text(1)?
        .unwrap_or_else(|| "No failure detail was recorded".into());
    Ok(Some(one_line_excerpt(&reason)))
}

/// Find the actual failed producer in the latest canonical execution. Descendants
/// settled by impossibility have no failed work turn or crashed run of their own.
pub async fn issue_failure_conn(
    conn: &cairn_db::storage::TrackedConnection,
    issue_id: &str,
) -> crate::storage::DbResult<Option<FailureEvidence>> {
    let mut rows = conn
        .query(
            "SELECT j.id, p.key, i.number, e.seq, COALESCE(j.uri_segment,j.node_name),
                j.recipe_node_id, e.snapshot, j.parent_job_id
         FROM issues i JOIN projects p ON p.id=i.project_id
         JOIN executions e ON e.id=(
             SELECT id FROM executions WHERE issue_id=i.id AND lifecycle='canonical'
             ORDER BY seq DESC, started_at DESC LIMIT 1)
         JOIN jobs j ON j.execution_id=e.id
         WHERE i.id=?1 AND j.status='failed'
           AND j.id=(SELECT latest.id FROM jobs latest
               WHERE latest.execution_id=j.execution_id
                 AND COALESCE(latest.recipe_node_id,latest.uri_segment,latest.node_name,latest.id)
                     =COALESCE(j.recipe_node_id,j.uri_segment,j.node_name,j.id)
               ORDER BY latest.created_at DESC, latest.rowid DESC LIMIT 1)
         ORDER BY j.updated_at DESC, j.id",
            (issue_id,),
        )
        .await?;
    while let Some(row) = rows.next().await? {
        // Inherited branches point parent_job_id at an upstream recipe job too.
        // Snapshot membership, not that pointer, identifies DAG producers.
        let node_id = row.opt_text(5)?;
        let snapshot = row.opt_text(6)?;
        let in_recipe = node_id.is_some()
            && snapshot
                .as_deref()
                .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok())
                .and_then(|value| value.get("recipe")?.get("nodes")?.as_array().cloned())
                .is_some_and(|nodes| {
                    nodes
                        .iter()
                        .any(|node| node.get("id").and_then(|id| id.as_str()) == node_id.as_deref())
                });
        if !in_recipe && row.opt_text(7)?.is_some() {
            continue;
        }
        let job_id = row.text(0)?;
        let Some(reason) = job_failure_reason_conn(conn, &job_id).await? else {
            continue;
        };
        let Some(segment) = row.opt_text(4)? else {
            continue;
        };
        let node_uri = cairn_uri::build_node_uri(
            &row.text(1)?,
            row.i64(2)? as i32,
            row.i64(3)? as i32,
            &segment,
        );
        return Ok(Some(FailureEvidence {
            job_id: Some(job_id),
            node_uri,
            reason,
        }));
    }
    drop(rows);
    let mut actions = conn
        .query(
            "SELECT p.key, i.number, e.seq, a.uri_segment, a.error_message
         FROM issues i JOIN projects p ON p.id=i.project_id
         JOIN executions e ON e.id=(
             SELECT id FROM executions WHERE issue_id=i.id AND lifecycle='canonical'
             ORDER BY seq DESC, started_at DESC LIMIT 1)
         JOIN action_runs a ON a.execution_id=e.id
         WHERE i.id=?1 AND a.status='failed' AND a.uri_segment IS NOT NULL
           AND a.id=(SELECT latest.id FROM action_runs latest
               WHERE latest.execution_id=a.execution_id AND latest.recipe_node_id=a.recipe_node_id
               ORDER BY latest.created_at DESC, latest.rowid DESC LIMIT 1)
         ORDER BY a.completed_at DESC, a.created_at DESC, a.id",
            (issue_id,),
        )
        .await?;
    if let Some(row) = actions.next().await? {
        return Ok(Some(FailureEvidence {
            job_id: None,
            node_uri: cairn_uri::build_node_uri(
                &row.text(0)?,
                row.i64(1)? as i32,
                row.i64(2)? as i32,
                &row.text(3)?,
            ),
            reason: one_line_excerpt(
                &row.opt_text(4)?
                    .unwrap_or_else(|| "No failure detail was recorded".into()),
            ),
        }));
    }
    Ok(None)
}

async fn resolved_issue_confirmation(
    runtime: &AttentionRuntime,
    issue_uri: &str,
) -> Option<String> {
    let project_key = cairn_uri::parse_uri(issue_uri)?.project_key()?.to_string();
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
                    let detail = match issue_failure_conn(conn, &issue.issue_id).await? {
                        Some(failure) => format!("{} — {}", failure.node_uri, failure.reason),
                        None => "No failure detail was recorded; inspect the child".into(),
                    };
                    return Ok(Some(format!(
                        "{}-{} \"{}\" failed: {detail}. Retry or delegate a fix.",
                        issue.project_key, issue.number, issue.title
                    )));
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

/// The frozen message a push carries, for every reader that needs it: the
/// agent's prompt and the composer's pending list must name the same message.
///
/// An unkeyed direct push names its message in the key (`direct:{message_id}`).
/// A system direct push records its message id as the push fingerprint
/// ([`crate::direct::insert_system_direct_push_with_message_id_conn`]), and one
/// under a stable key (`schedule:`, `tenet-verdict:`, `gate:`, ...) supersedes
/// in place, so there the fingerprint, not the key, names the message the push
/// currently carries. The fingerprint only counts when it names a direct-channel
/// message: review and resolution pushes use the same column for a source-state
/// fingerprint. `message` reads a message row by id from `db`, the database the
/// push lives in.
pub async fn direct_push_message<F, Fut>(
    db: &LocalDb,
    push: &crate::push::Push,
    message: F,
) -> Option<cairn_domain::Message>
where
    F: Fn(String) -> Fut,
    Fut: std::future::Future<Output = Option<cairn_domain::Message>>,
{
    if let Some(message_id) = crate::push::direct_message_id_from_key(&push.key) {
        if let Some(msg) = message(message_id.to_string()).await {
            return Some(msg);
        }
    }
    let message_id = crate::push::latest_push_fingerprint(db, &push.recipient, &push.key)
        .await
        .ok()
        .flatten()
        .flatten()?;
    message(message_id)
        .await
        .filter(|msg| msg.channel_type == crate::models::ChannelType::Direct)
}

async fn pushed_direct_message(
    runtime: &AttentionRuntime,
    push: &crate::push::Push,
) -> Option<cairn_domain::Message> {
    direct_push_message(&runtime.private_db, push, move |message_id| async move {
        runtime.host.message(&runtime.private_db, &message_id).await
    })
    .await
}

/// Render a drained attention push as a compact, reference-first update.
///
/// Direct messages, catch-up digests and the child's own chat line, review
/// titles and summaries, terminal resolutions, check verdicts, and posts have
/// bounded, targeted database lookups because their essential detail is not
/// represented by the resource URI alone. Each is at most a few indexed
/// statements, and any miss degrades to the bare pointer, never an error. Other
/// push kinds deliberately do not render their full resource here: resume
/// prompt assembly holds the launch lock, and making a resource read per pending
/// push made user messages wait tens of seconds. The URI remains the canonical
/// route to the live content through the read tool.
pub async fn render_push_resolved(runtime: &AttentionRuntime, push: &crate::push::Push) -> String {
    let header = format!(
        "Attention update ({}): {}",
        push.wake.as_str(),
        push.content_ref
    );
    // A schedule wake's content is the reason its recipient wrote for itself.
    // Name that reason in the headline: naming only the referent tells the reader
    // nothing they did not already know.
    if push.key.starts_with("schedule:") {
        if let Some(msg) = pushed_direct_message(runtime, push).await {
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
                let body = direct_message_body(runtime, &msg).await;
                return format!("{schedule_header}\n\n{body}");
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

    // A `child-unresumed:` push is the same kind of statement: which turn the
    // restart took, what became of the resume, and that the work is the parent's
    // to pick up. Re-derived at delivery, so a child that has since moved on says
    // so rather than sending the parent to read a healthy node.
    if push
        .key
        .starts_with(&format!("{}:", crate::push::CHILD_UNRESUMED_PUSH_PREFIX))
    {
        if let Some(body) = runtime
            .host
            .render_content(ContentRequest::ChildUnresumed {
                recipient: &push.recipient,
                reference: &push.content_ref,
            })
            .await
        {
            return format!("{header}\n\n{body}");
        }
    }

    // A `child-unowned:` push says a child is marked running with nothing moving
    // it and Cairn could not continue it, and why. Re-derived at delivery for the
    // same reason as `child-unresumed:`.
    if push
        .key
        .starts_with(&format!("{}:", crate::push::CHILD_UNOWNED_PUSH_PREFIX))
    {
        if let Some(body) = runtime
            .host
            .render_content(ContentRequest::ChildUnowned {
                recipient: &push.recipient,
                reference: &push.content_ref,
            })
            .await
        {
            return format!("{header}\n\n{body}");
        }
    }

    // A `direct:` push carries frozen message content, not an idempotent
    // resolvable referent. Resolve it from the durable `messages` row rather
    // than from `content_ref` (which is the conversation surface the wake card
    // links to). Falls back to the header line if the row is missing
    // (CAIRN-1900).
    if push.key.starts_with("direct:") {
        return match pushed_direct_message(runtime, push).await {
            Some(msg) => {
                let body = direct_message_body(runtime, &msg).await;
                format!("{header}\n\n{body}")
            }
            _ => header,
        };
    }
    // A `catchup:` push says what the child has been doing without rendering the
    // potentially enormous chat window: the operator messages the reader has not
    // caught up on, then one line from the child's own chat since the cursor.
    // With neither, the pointer stands. The URI remains available for follow-up.
    if let Some(child_job_id) = push.key.strip_prefix("catchup:") {
        let db = runtime
            .host
            .database_for_job(child_job_id)
            .await
            .unwrap_or_else(|_| runtime.private_db.clone());
        let digest = catchup_operator_digest(&db, &push.recipient, child_job_id).await;
        let line = catchup_chat_line(&db, &push.content_ref, child_job_id).await;
        let body = [digest, line]
            .into_iter()
            .flatten()
            .collect::<Vec<_>>()
            .join("\n\n");
        if !body.is_empty() {
            return format!("{header}\n\n{body}");
        }
    }
    // A `review:` push's fingerprint is a source-state marker (`artifact:`,
    // `sha:`), never a message, so it skips the direct-message lookup below.
    if push.key.starts_with("review:") {
        return match review_artifact_line(runtime, &push.content_ref).await {
            Some(body) => format!("{header}\n\n{body}"),
            None => pointer_update(&header, push),
        };
    }
    // A system direct push under its own coalescing key (`automation:`,
    // `condition:`, `gate:`, `tenet-verdict:`, `tenet-contradiction:`, ...) has
    // the recipient's own home as its referent, which tells the recipient
    // nothing; the whole fact is the frozen message. It is recognized by what it
    // carries, not by a list of prefixes: a list missed every producer added
    // after it, and each of those reached its recipient as a pointer home.
    if let Some(msg) = pushed_direct_message(runtime, push).await {
        let body = direct_message_body(runtime, &msg).await;
        return format!("{header}\n\n{body}");
    }
    pointer_update(&header, push)
}

/// The bare reference-first update: the push kind's headline and the address to
/// read. What every detail lookup degrades to.
fn pointer_update(header: &str, push: &crate::push::Push) -> String {
    let prefix = push
        .key
        .split_once(':')
        .map(|(prefix, _)| prefix)
        .unwrap_or(&push.key);
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
