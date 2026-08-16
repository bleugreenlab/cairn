//! Post attention: who a post, its comments, and the canonical references
//! written into them reach.
//!
//! Three facts share one source kind ([`SOURCE_KIND_POST`]) and one delivery
//! mechanism — the attention push:
//!
//! - [`FACT_KIND_NEW_POST`] reaches the nodes that elected to watch Posts
//!   (`{subscribe:{kind:"posts"}}`), filtered by jurisdiction.
//! - [`FACT_KIND_POST_COMMENT`] reaches the post author's home.
//! - [`FACT_KIND_POST_MENTION`] reaches each canonical destination the text
//!   names.
//!
//! Nothing here is a second notification system. A post fact becomes an
//! `attention_pushes` row like every other fact, so mute is the same
//! downgrade-at-creation rule (a muted subscriber's `Wake` becomes `Passive` and
//! rides along on its next run instead of rousing it — CAIRN-1900), supersession
//! is the same `(recipient, key)` collapse, and delivery is the same drain.
//!
//! **Recipients are resolved, never stored.** A post records its author as a
//! principal naming a durable home URI, and a mention names a home or an issue.
//! Both are resolved to a live job at delivery time through the canonical
//! resolvers ([`watcher_jobs_for_issue`], `job_id_for_node_coordinate`, the
//! thread session helpers), so a thread that has rotated to a new session
//! receives on the new one and a retired coordinator stops receiving.

use cairn_common::identity::PrincipalRef;
use cairn_common::uri::{CairnResource, UriMatch};
use cairn_db::turso::params;

use crate::messages::queued::DeliveryUrgency;
use crate::models::{Post, PostComment};
use crate::orchestrator::attention_push::{Boundary, Wake};
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, RowExt};

use super::child::watcher_jobs_for_issue;
use super::matching::{subscription_accepts_fact, subscriptions_for_source};
use super::types::*;

/// Push key prefix for a new post reaching a feed subscriber.
pub(crate) const NEW_POST_PUSH_PREFIX: &str = "post";
/// Push key prefix for a comment reaching the post author.
pub(crate) const POST_COMMENT_PUSH_PREFIX: &str = "post-comment";
/// Push key prefix for a canonical reference reaching what it names.
pub(crate) const POST_MENTION_PUSH_PREFIX: &str = "post-mention";

/// The `(source kind, fact kind)` a post push key prefix stands for, or `None`
/// when the prefix is not a post push.
///
/// The one place the push-key vocabulary and the wake-subscription vocabulary
/// are tied together, so the central mute consultation in
/// [`crate::orchestrator::attention_push::push_with_fingerprint`] can ask the
/// subscription registry about a post push without restating either.
pub(crate) fn post_push_source(prefix: &str) -> Option<(&'static str, &'static str)> {
    match prefix {
        NEW_POST_PUSH_PREFIX => Some((SOURCE_KIND_POST, FACT_KIND_NEW_POST)),
        POST_COMMENT_PUSH_PREFIX => Some((SOURCE_KIND_POST, FACT_KIND_POST_COMMENT)),
        POST_MENTION_PUSH_PREFIX => Some((SOURCE_KIND_POST, FACT_KIND_POST_MENTION)),
        _ => None,
    }
}

/// What routing one post event actually did.
///
/// `recorded` names only the recipients whose push row was durably written, so a
/// caller can report attention without overstating it; `failed` counts the
/// recipients whose row could not be written and who were therefore told
/// nothing.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PostAttention {
    /// Recipients whose attention push was durably recorded, in delivery order.
    pub recorded: Vec<String>,
    /// Of [`Self::recorded`], those an idle-resume nudge is owed to. A muted
    /// subscriber is absent: its row was created `Passive` and rides along.
    pub woken: Vec<String>,
    /// Recipients whose push could not be recorded. Nothing reached them.
    pub failed: usize,
}

fn post_uri(post_id: i64) -> String {
    format!("cairn://posts/{post_id}")
}

/// Route a newly created post, then wake the idle recipients.
///
/// Best-effort by contract: the `posts` row is the durable content boundary and
/// is already committed when this runs. A routing failure is reported to the
/// caller and logged; it never unwrites the post.
pub async fn route_new_post(orch: &Orchestrator, post: &Post) -> Result<PostAttention, String> {
    let attention = record_new_post_attention(&orch.db.local, post).await?;
    wake_recipients(orch, &attention);
    Ok(attention)
}

/// Route a newly created comment, then wake the idle recipients.
pub async fn route_post_comment(
    orch: &Orchestrator,
    post: &Post,
    comment: &PostComment,
) -> Result<PostAttention, String> {
    let attention = record_post_comment_attention(&orch.db.local, post, comment).await?;
    wake_recipients(orch, &attention);
    Ok(attention)
}

fn wake_recipients(orch: &Orchestrator, attention: &PostAttention) {
    if attention.recorded.is_empty() {
        return;
    }
    orch.notifier.emit_change("attention_pushes");
    for recipient in &attention.woken {
        if let Err(error) = crate::messages::delivery::nudge_job_for_urgency(
            orch,
            recipient,
            DeliveryUrgency::Steer,
        ) {
            log::warn!(
                "post attention wake for {} failed: {error}",
                &recipient[..recipient.len().min(8)]
            );
        }
    }
}

/// Record the attention a new post owes: the destinations its text names, then
/// the Posts feed subscribers its scope admits.
///
/// Mentions are routed first deliberately. A node that is both mentioned and
/// subscribed qualifies twice, and one wake is owed, not two — so the more
/// specific reason claims the recipient and the feed pass skips it.
pub(crate) async fn record_new_post_attention(
    db: &LocalDb,
    post: &Post,
) -> Result<PostAttention, String> {
    let uri = post_uri(post.id);
    let scope = post.project_id.as_deref();
    let author_home = home_job_for_principal(db, &post.author).await;
    let mut text = post.content.clone();
    if let Some(title) = &post.title {
        text.push('\n');
        text.push_str(title);
    }
    let mentioned = mention_recipients(db, &text, scope).await?;
    let subscribers = post_feed_recipients(db, scope).await?;
    Ok(record_post_attention(
        db,
        &uri,
        &[
            (POST_MENTION_PUSH_PREFIX, mentioned),
            (NEW_POST_PUSH_PREFIX, subscribers),
        ],
        author_home.as_deref(),
    )
    .await)
}

/// Record the attention a new comment owes: the destinations the comment names,
/// then the post author's current home.
///
/// A comment is not a feed event — it reaches the author and whoever the comment
/// itself names, not everyone watching Posts.
pub(crate) async fn record_post_comment_attention(
    db: &LocalDb,
    post: &Post,
    comment: &PostComment,
) -> Result<PostAttention, String> {
    let uri = post_uri(post.id);
    let scope = post.project_id.as_deref();
    let commenter_home = home_job_for_principal(db, &comment.author).await;
    let mentioned = mention_recipients(db, &comment.content, scope).await?;
    let author = match home_job_for_principal(db, &post.author).await {
        Some(job) if job_may_see_post(db, &job, scope).await? => vec![job],
        _ => Vec::new(),
    };
    Ok(record_post_attention(
        db,
        &uri,
        &[
            (POST_MENTION_PUSH_PREFIX, mentioned),
            (POST_COMMENT_PUSH_PREFIX, author),
        ],
        commenter_home.as_deref(),
    )
    .await)
}

/// Write one push per recipient, in route order, skipping the originating node
/// and any recipient an earlier route already claimed.
///
/// The dedup is why this is one function rather than a loop at each call site:
/// repeated references to the same destination, and a destination that qualifies
/// through two different routes, must produce exactly one wake. Within a single
/// route the `(recipient, key)` supersession in the push store does the same job
/// across separate events — a second comment before the first is drained
/// refreshes the pending row instead of stacking a second wake.
///
/// Each push is its own write, and a failure is contained to its recipient: the
/// others still land, and the count of the ones that did not is reported rather
/// than swallowed.
async fn record_post_attention(
    db: &LocalDb,
    uri: &str,
    routes: &[(&'static str, Vec<String>)],
    exclude: Option<&str>,
) -> PostAttention {
    let mut attention = PostAttention::default();
    for (prefix, recipients) in routes {
        let key = format!("{prefix}:{uri}");
        for recipient in recipients {
            if Some(recipient.as_str()) == exclude || attention.recorded.contains(recipient) {
                continue;
            }
            match crate::orchestrator::attention_push::push(
                db,
                recipient,
                uri,
                Wake::Wake,
                Boundary::Event,
                &key,
            )
            .await
            {
                Ok((_, effective)) => {
                    if effective.wakes_idle() {
                        attention.woken.push(recipient.clone());
                    }
                    attention.recorded.push(recipient.clone());
                }
                Err(error) => {
                    attention.failed += 1;
                    log::warn!("post attention push to {recipient} for {uri} failed: {error}");
                }
            }
        }
    }
    attention
}

/// The jobs whose elective Posts watch admits a post of this scope.
///
/// A row is a standing watch over the corpus, so the filter that decides what it
/// actually receives is applied here, at delivery: a workspace-wide post may
/// reach every subscriber, a project-scoped post reaches only subscribers whose
/// home is in that project. Muted rows are included — mute governs how loudly a
/// recipient is told, not whether it is a recipient — and `unsubscribed` rows
/// are excluded, exactly as [`watcher_jobs_for_issue`] treats the issue axis.
async fn post_feed_recipients(
    db: &LocalDb,
    project_id: Option<&str>,
) -> Result<Vec<String>, String> {
    let mut jobs: Vec<String> = Vec::new();
    for subscription in subscriptions_for_source(db, SOURCE_KIND_POST, None).await? {
        if subscription.state == WakeSubscriptionState::Unsubscribed
            || jobs.contains(&subscription.job_id)
            || !subscription_accepts_fact(&subscription, FACT_KIND_NEW_POST)
            || !job_may_see_post(db, &subscription.job_id, project_id).await?
        {
            continue;
        }
        jobs.push(subscription.job_id);
    }
    Ok(jobs)
}

/// Jurisdiction, applied before every delivery including mention routes: a
/// workspace-wide post may reach any home; a project-scoped post reaches only
/// homes in its own project. A job whose home project cannot be resolved is not
/// admitted to a scoped post.
async fn job_may_see_post(
    db: &LocalDb,
    job_id: &str,
    project_id: Option<&str>,
) -> Result<bool, String> {
    let Some(project_id) = project_id else {
        return Ok(true);
    };
    Ok(home_project_id_for_job(db, job_id).await?.as_deref() == Some(project_id))
}

/// The project a job's home belongs to. An issue job takes its issue's project;
/// a thread session (which has no issue) carries its own — the same COALESCE
/// `home_uri_for_job_conn` names a job through.
async fn home_project_id_for_job(db: &LocalDb, job_id: &str) -> Result<Option<String>, String> {
    let job_id = job_id.to_string();
    db.read(|conn| {
        let job_id = job_id.clone();
        Box::pin(async move {
            let mut rows = conn
                .query(
                    "SELECT COALESCE(i.project_id, j.project_id)
                     FROM jobs j
                     LEFT JOIN issues i ON i.id = j.issue_id
                     WHERE j.id = ?1 LIMIT 1",
                    params![job_id.as_str()],
                )
                .await?;
            match rows.next().await? {
                Some(row) => row.opt_text(0),
                None => Ok(None),
            }
        })
    })
    .await
    .map_err(|error| format!("Failed to resolve a job's home project: {error}"))
}

/// Every job the canonical references in `text` address, deduplicated and
/// jurisdiction-filtered.
///
/// Extraction is [`cairn_common::uri::scan_uris`] — the one text scanner, which
/// hands each candidate to the one URI parser. Nothing that fails to parse as a
/// canonical resource is a reference, so prose about "cairn://" routes nowhere,
/// and a caller cannot name a raw job id at all: the only addresses that resolve
/// are the ones the URI grammar admits, resolved through the same resolvers the
/// resource graph uses.
async fn mention_recipients(
    db: &LocalDb,
    text: &str,
    project_id: Option<&str>,
) -> Result<Vec<String>, String> {
    let mut jobs: Vec<String> = Vec::new();
    for found in cairn_common::uri::scan_uris(text) {
        for job in mention_destinations(db, &found).await? {
            if jobs.contains(&job) || !job_may_see_post(db, &job, project_id).await? {
                continue;
            }
            jobs.push(job);
        }
    }
    Ok(jobs)
}

/// The jobs one canonical reference addresses.
///
/// An issue reaches its watchers — the same derived-plus-explicit set every
/// issue-sourced push uses. A node, a sub-agent task, and a thread reach the job
/// that is that home right now. Every other resource shape is a citation rather
/// than a destination: a link to a diff or a chat window says where to look, not
/// who to tell, and routing it would wake a node for being mentioned in passing.
async fn mention_destinations(db: &LocalDb, found: &UriMatch) -> Result<Vec<String>, String> {
    match &found.resource {
        CairnResource::Issue { project, number } => {
            watcher_jobs_for_issue(db, &cairn_common::uri::build_issue_uri(project, *number)).await
        }
        CairnResource::Node { .. } | CairnResource::Task { .. } | CairnResource::Thread { .. } => {
            Ok(home_job_for_resource(db, &found.resource)
                .await?
                .into_iter()
                .collect())
        }
        _ => Ok(Vec::new()),
    }
}

/// The job that is a stored principal's home **right now**.
///
/// A post's author is recorded as a principal naming a durable home URI, never a
/// job id, precisely so this resolution happens at delivery: a thread whose
/// session has rotated receives on the session it is on now, and a node whose
/// job is gone receives nothing rather than something stale. Only an agent
/// principal has a home to resume; a human or external author has no session for
/// a wake to reach.
pub(crate) async fn home_job_for_principal(
    db: &LocalDb,
    principal: &PrincipalRef,
) -> Option<String> {
    let PrincipalRef::Agent { node, .. } = principal else {
        return None;
    };
    let resource = cairn_common::uri::parse_uri(node)?;
    home_job_for_resource(db, &resource).await.ok().flatten()
}

/// A home URI's current job, through the canonical coordinate resolvers.
///
/// Threads resolve through their own helpers rather than the node coordinate so
/// the *newest* session wins after a rotation, which is the whole point of
/// resolving late.
async fn home_job_for_resource(
    db: &LocalDb,
    resource: &CairnResource,
) -> Result<Option<String>, String> {
    let resolved = match resource {
        CairnResource::Node {
            project,
            number,
            exec_seq,
            node_id,
        } => {
            crate::jobs::queries::job_id_for_node_coordinate(
                db, project, *number, *exec_seq, node_id, None,
            )
            .await
        }
        CairnResource::Task {
            project,
            number,
            exec_seq,
            node_id,
            task_name,
        } => {
            crate::jobs::queries::job_id_for_node_coordinate(
                db,
                project,
                *number,
                *exec_seq,
                node_id,
                Some(task_name),
            )
            .await
        }
        CairnResource::Thread {
            project,
            name,
            path,
        } => {
            let (project, name) = (project.clone(), name.clone());
            // Only the thread itself and the tasks beneath it are homes; a
            // deeper sub-resource path is a citation of one of its surfaces.
            let task = match path.as_slice() {
                [] => None,
                [task, segment] if task == "task" => Some(segment.clone()),
                _ => return Ok(None),
            };
            db.read(move |conn| {
                let (project, name, task) = (project.clone(), name.clone(), task.clone());
                Box::pin(async move {
                    match task {
                        None => {
                            crate::threads::session_job_id_by_name_conn(conn, &project, &name).await
                        }
                        Some(task) => {
                            crate::threads::task_job_id_by_name_conn(conn, &project, &name, &task)
                                .await
                        }
                    }
                })
            })
            .await
        }
        _ => return Ok(None),
    };
    resolved.map_err(|error| format!("Failed to resolve a post reference's home: {error}"))
}
