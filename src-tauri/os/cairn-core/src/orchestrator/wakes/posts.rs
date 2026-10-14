//! Posts on the fact log.
//!
//! A post and a comment are each minted as one fact log row where their
//! `posts` or `post_comments` row is written, and minting is all this module
//! does on the write path. Who a row reaches is the log worker's decision, by
//! the rules every source shares: the registry names the fields that address
//! recipients (`post_created.mentions`, `post_commented.postAuthor` and
//! `.mentions`), a Posts watch is an ordinary `{fact: post_created}`
//! consumer, and jurisdiction, the actor's own home, one wake per recipient
//! per row, and coalescing are the index lane's.
//!
//! **A row stores addresses, never recipients.** An author is stored as the
//! home URI its principal names and a mention as the canonical URI the text
//! wrote; the job each reaches is resolved at delivery
//! ([`super::addressed`]), so a thread that rotated to a new session receives
//! on the new one.
//!
//! **The wake carries the writing.** The row holds no body. The wake renders
//! the post or comment from its canonical record ([`render_post_wake`]).

use std::collections::BTreeMap;

use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_common::uri::CairnResource;
use serde_json::Value;

use crate::models::{Post, PostComment};
use crate::orchestrator::Orchestrator;
use crate::resources::posts::{
    find_post_comment, render_comment, render_post, PostContext, PostForm,
};
use crate::routes::{Mint, RouteFact};
use crate::storage::LocalDb;

/// The fact source a new post is minted on.
pub const POST_CREATED: &str = "post_created";
/// The fact source a new comment is minted on.
pub const POST_COMMENTED: &str = "post_commented";

fn post_uri(post_id: i64) -> String {
    format!("cairn://posts/{post_id}")
}

/// The home a stored principal names, when it has one a wake could reach.
/// Only an agent has a home to resume; a human or external author has none.
fn principal_home(principal: &PrincipalRef) -> Option<String> {
    let PrincipalRef::Agent { node, .. } = principal else {
        return None;
    };
    cairn_common::uri::parse_uri(node).map(|home| home.to_uri())
}

fn first_line(text: &str) -> &str {
    text.lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
}

async fn project_key(db: &LocalDb, project_id: Option<&str>) -> Option<String> {
    db.query_opt_text(
        "SELECT key FROM projects WHERE id = ?1",
        (project_id?.to_string(),),
    )
    .await
    .ok()
    .flatten()
}

struct PostFact {
    source: &'static str,
    record_ref: String,
    record_id: i64,
    fields: BTreeMap<String, Value>,
    summary: String,
    author: PrincipalRef,
    appearance: AppearanceSnapshot,
    project_id: Option<String>,
    project_key: Option<String>,
}

/// Mint one post fact and return the canonical URI of its event row, or
/// `None` when the log already holds it.
async fn mint_post_fact(
    orch: &Orchestrator,
    fact: PostFact,
    actor_job_id: Option<&str>,
) -> Result<Option<String>, String> {
    let id = crate::routes::mint(
        orch,
        Mint {
            fact: RouteFact {
                source: fact.source.to_string(),
                identity: fact.record_ref.clone(),
                fields: fact.fields,
                origin: crate::issues::crud::IssueAuthorship::new(fact.author, fact.appearance)
                    .ok(),
                summary: Some(fact.summary),
                route_provenance: None,
            },
            record_ref: fact.record_ref,
            record_id: Some(fact.record_id.to_string()),
            project_id: fact.project_id.as_deref(),
            presence: crate::routes::log::operator_presence().await,
            actor_job_id,
        },
    )
    .await?;
    Ok(id.map(|id| match fact.project_key.as_deref() {
        Some(key) => cairn_common::uri::build_project_event_uri(key, id),
        None => cairn_common::uri::build_workspace_event_uri(id),
    }))
}

/// Mint a `post_created` fact for a post whose row is already committed.
///
/// Best-effort by contract: the `posts` row is the durable content boundary, so
/// a mint failure is reported to the caller and never unwrites the post.
/// `actor_job_id` is the job that wrote it, whose own home the post never wakes.
pub async fn mint_post_created(
    orch: &Orchestrator,
    post: &Post,
    actor_job_id: Option<&str>,
) -> Result<Option<String>, String> {
    let db = &orch.db.local;
    let uri = post_uri(post.id);
    let key = project_key(db, post.project_id.as_deref()).await;
    let mut text = post.content.clone();
    if let Some(title) = &post.title {
        text.push('\n');
        text.push_str(title);
    }
    let mentions = crate::routes::structural_mentions(db, &text).await;
    let mut fields = BTreeMap::from([
        ("postUri".to_string(), Value::String(uri.clone())),
        (
            "scope".to_string(),
            Value::String(match (&post.project_id, &key) {
                (None, _) => "workspace".to_string(),
                (Some(_), Some(key)) => key.clone(),
                (Some(id), None) => id.clone(),
            }),
        ),
        (
            "mentions".to_string(),
            Value::Array(mentions.into_iter().map(Value::String).collect()),
        ),
    ]);
    if let Some(key) = &key {
        fields.insert("project".into(), Value::String(key.clone()));
    }
    if let Some(author) = principal_home(&post.author) {
        fields.insert("author".into(), Value::String(author));
    }
    if let Some(title) = &post.title {
        fields.insert("title".into(), Value::String(title.clone()));
    }
    mint_post_fact(
        orch,
        PostFact {
            source: POST_CREATED,
            record_ref: uri,
            record_id: post.id,
            fields,
            summary: post
                .title
                .clone()
                .unwrap_or_else(|| first_line(&post.content).to_string()),
            author: post.author.clone(),
            appearance: post.appearance.clone(),
            project_id: post.project_id.clone(),
            project_key: key,
        },
        actor_job_id,
    )
    .await
}

/// Mint a `post_commented` fact for a comment whose row is already committed.
/// Same contract as [`mint_post_created`]. The comment takes its post's scope.
pub async fn mint_post_commented(
    orch: &Orchestrator,
    post: &Post,
    comment: &PostComment,
    actor_job_id: Option<&str>,
) -> Result<Option<String>, String> {
    let db = &orch.db.local;
    let uri = cairn_common::uri::build_post_comment_uri(post.id, comment.id);
    let key = project_key(db, post.project_id.as_deref()).await;
    let mentions = crate::routes::structural_mentions(db, &comment.content).await;
    let mut fields = BTreeMap::from([
        ("postUri".to_string(), Value::String(post_uri(post.id))),
        ("commentUri".to_string(), Value::String(uri.clone())),
        (
            "mentions".to_string(),
            Value::Array(mentions.into_iter().map(Value::String).collect()),
        ),
    ]);
    if let Some(key) = &key {
        fields.insert("project".into(), Value::String(key.clone()));
    }
    if let Some(author) = principal_home(&comment.author) {
        fields.insert("author".into(), Value::String(author));
    }
    if let Some(author) = principal_home(&post.author) {
        fields.insert("postAuthor".into(), Value::String(author));
    }
    mint_post_fact(
        orch,
        PostFact {
            source: POST_COMMENTED,
            record_ref: uri,
            record_id: comment.id,
            fields,
            summary: format!(
                "Comment on {}: {}",
                post.title.as_deref().unwrap_or("Untitled"),
                first_line(&comment.content)
            ),
            author: comment.author.clone(),
            appearance: comment.appearance.clone(),
            project_id: post.project_id.clone(),
            project_key: key,
        },
        actor_job_id,
    )
    .await
}

/// The body a post wake carries: the writing itself, not an errand.
///
/// A post is short and a wake exists to be classified, ignorable or not, so
/// sending a recipient somewhere else to read five words costs it a whole read
/// to learn nothing. The post, or the comment under its post's title, is
/// rendered through the same renderer `cairn://posts/{id}` uses, bounded by
/// [`PostForm::Wake`], with the canonical URI kept as the pointer past the
/// bound.
///
/// `None` when `record_ref` names no post or comment that still exists, so the
/// caller keeps its reference-only header rather than printing a lie.
pub(crate) async fn render_post_wake(db: &LocalDb, record_ref: &str) -> Option<String> {
    let (post_id, comment_id) = match cairn_common::uri::parse_uri(record_ref)? {
        CairnResource::Post { id } => (id, None),
        CairnResource::PostComment {
            post_id,
            comment_id,
        } => (post_id, Some(comment_id)),
        _ => return None,
    };
    let post = db.get_post(post_id).await.ok()??;
    let context = PostContext::resolve(db, std::slice::from_ref(&post)).await;
    match comment_id {
        None => Some(render_post(&post, &context, PostForm::Wake)),
        Some(comment_id) => {
            let comment = find_post_comment(db, post_id, comment_id).await.ok()??;
            Some(format!(
                "On [{}](cairn://posts/{post_id}):\n\n{}",
                post.title.as_deref().unwrap_or("Untitled"),
                render_comment(&comment, &context, PostForm::Wake)
            ))
        }
    }
}
