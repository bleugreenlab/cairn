//! Publishing posts and comments: the one write path every surface calls.
//!
//! An agent writes through `cairn://posts` or a camp's `posts` collection, and
//! the operator through the desktop's `create_post`. Those entry points differ
//! only in how the author is authenticated and how the target is named; each
//! hands [`publish_post`] an already-authenticated author and a [`PostTarget`],
//! and this module validates the target, writes the row, announces it, and
//! mints its fact. Nothing else inserts a post.
//!
//! A camp post is an ordinary project post that also names one camp of that
//! project. Which camp is checked here, because the reference crosses
//! databases (posts are workspace-private, camps live with their project) and
//! no foreign key can hold it: the camp must exist in that project and be live,
//! and an agent may address only a camp its thread takes part in. The operator
//! is an implicit participant of every camp. Base camp is a room: every thread
//! of the project may post to it.
//!
//! An operator's camp post is minted like any other post fact, with no actor
//! job: it reaches the camp's participants by role through the fact log, never
//! as a direct operator message to any of them.

use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};

use crate::models::{Camp, CreatePost, CreatePostComment, Post, PostComment};
use crate::orchestrator::Orchestrator;
use crate::storage::RowExt;

/// Where a post is filed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PostTarget {
    /// The workspace-wide corpus.
    Workspace,
    /// One project, by its row id.
    Project { project_id: String },
    /// One camp of one project, by the camp's immutable id.
    Camp { project_id: String, camp_id: String },
}

/// A post to publish. `actor_job_id` is the agent job writing it, or `None`
/// for the operator; it decides who may address a camp, and the job it names
/// is never woken by its own post.
pub struct PublishPost {
    pub target: PostTarget,
    pub title: Option<String>,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub actor_job_id: Option<String>,
}

/// A comment to publish, under the same authorship rules as a post.
pub struct PublishComment {
    pub post_id: i64,
    pub content: String,
    pub author: PrincipalRef,
    pub appearance: AppearanceSnapshot,
    pub actor_job_id: Option<String>,
}

/// Why a publish did not write.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PublishError {
    /// The caller asked for something it may not do; nothing was written.
    Refused(String),
    /// Cairn failed while checking or writing.
    Internal(String),
}

impl std::fmt::Display for PublishError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(message) | Self::Internal(message) => f.write_str(message),
        }
    }
}

/// What a publish wrote: the row, and the outcome of minting its fact. The row
/// is the durable content boundary, so a mint failure is reported beside it
/// and never unwrites it.
pub struct Published<T> {
    pub record: T,
    pub event: Result<Option<String>, String>,
}

const NON_THREAD_CAMP_POST: &str = "only a thread posts to a camp: this run acts for no thread. Post at project scope (scope:\"project\"), or ask the thread that owns this work to post to the camp.";

fn internal(error: impl std::fmt::Display) -> PublishError {
    PublishError::Internal(error.to_string())
}

/// The camp a post addresses, checked: it exists in `project_id`, is live,
/// and the author may post to it.
async fn check_camp_target(
    orch: &Orchestrator,
    project_id: &str,
    camp_id: &str,
    actor_job_id: Option<&str>,
) -> Result<Camp, PublishError> {
    let db = crate::projects::crud::owning_db(&orch.db, project_id)
        .await
        .map_err(internal)?;
    let camp = {
        let camp_id = camp_id.to_string();
        db.read(move |conn| {
            let camp_id = camp_id.clone();
            Box::pin(async move { cairn_db::storage::camps::camp_by_id_conn(conn, &camp_id).await })
        })
        .await
        .map_err(internal)?
    };
    let Some(camp) = camp.filter(|camp| camp.project_id == project_id) else {
        return Err(PublishError::Refused(
            "no such camp in this project; a post addresses a camp of its own project only"
                .to_string(),
        ));
    };
    if camp.is_struck() {
        return Err(PublishError::Refused(format!(
            "camp `{}` is struck; it is read-only and takes no new posts",
            camp.slug
        )));
    }
    let Some(job_id) = actor_job_id else {
        // The operator takes part in every camp of their workspace.
        return Ok(camp);
    };
    let thread = db
        .query_opt(
            "SELECT j.thread_id, t.project_id FROM jobs j JOIN threads t ON t.id = j.thread_id
             WHERE j.id = ?1",
            (job_id.to_string(),),
            |row| Ok((row.text(0)?, row.text(1)?)),
        )
        .await
        .map_err(internal)?;
    let Some((thread_id, thread_project)) = thread else {
        return Err(PublishError::Refused(NON_THREAD_CAMP_POST.to_string()));
    };
    if thread_project != project_id {
        return Err(PublishError::Refused(
            "a thread posts to its own project's camps only".to_string(),
        ));
    }
    if camp.is_base() {
        // Base camp is a room: every thread of the project is seated in it.
        return Ok(camp);
    }
    let takes_part = db
        .camp_participants(&camp)
        .await
        .map_err(internal)?
        .iter()
        .any(|participant| participant.subject.thread_id() == Some(thread_id.as_str()));
    if !takes_part {
        return Err(PublishError::Refused(format!(
            "your thread does not take part in camp `{}`; join it first by appending to its participants",
            camp.slug
        )));
    }
    Ok(camp)
}

fn emit_db_change(orch: &Orchestrator, table: &str) {
    if let Err(error) = orch.services.emitter.emit(
        "db-change",
        serde_json::json!({"table": table, "action": "insert"}),
    ) {
        tracing::warn!(%error, table, "failed to emit Posts db-change notification");
    }
}

/// Validate `input.target`, write the post, announce it, and mint its fact.
/// A refused publish writes nothing.
pub async fn publish_post(
    orch: &Orchestrator,
    input: PublishPost,
) -> Result<Published<Post>, PublishError> {
    let (project_id, camp_id) = match &input.target {
        PostTarget::Workspace => (None, None),
        PostTarget::Project { project_id } => (Some(project_id.clone()), None),
        PostTarget::Camp {
            project_id,
            camp_id,
        } => {
            let camp =
                check_camp_target(orch, project_id, camp_id, input.actor_job_id.as_deref()).await?;
            (Some(project_id.clone()), Some(camp.id))
        }
    };
    let post = orch
        .db
        .local
        .create_post(CreatePost {
            project_id,
            camp_id,
            title: input.title,
            content: input.content,
            author: input.author,
            appearance: input.appearance,
        })
        .await
        .map_err(internal)?;
    emit_db_change(orch, "posts");
    let event =
        crate::orchestrator::wakes::mint_post_created(orch, &post, input.actor_job_id.as_deref())
            .await;
    if let Err(error) = &event {
        tracing::warn!(post = post.id, %error, "post fact mint failed");
    }
    Ok(Published {
        record: post,
        event,
    })
}

/// Write a comment on an existing post, announce it, and mint its fact. A
/// comment on a struck camp's post is refused: the camp is read-only.
pub async fn publish_comment(
    orch: &Orchestrator,
    input: PublishComment,
) -> Result<Published<PostComment>, PublishError> {
    let post = orch
        .db
        .local
        .get_post(input.post_id)
        .await
        .map_err(internal)?
        .ok_or_else(|| PublishError::Refused(format!("post {} does not exist", input.post_id)))?;
    if let (Some(project_id), Some(camp_id)) = (&post.project_id, &post.camp_id) {
        let db = crate::projects::crud::owning_db(&orch.db, project_id)
            .await
            .map_err(internal)?;
        let camp_id = camp_id.clone();
        let camp = db
            .read(move |conn| {
                let camp_id = camp_id.clone();
                Box::pin(
                    async move { cairn_db::storage::camps::camp_by_id_conn(conn, &camp_id).await },
                )
            })
            .await
            .map_err(internal)?;
        if let Some(camp) = camp.filter(Camp::is_struck) {
            return Err(PublishError::Refused(format!(
                "camp `{}` is struck; its posts are read-only",
                camp.slug
            )));
        }
    }
    let comment = orch
        .db
        .local
        .create_post_comment(CreatePostComment {
            post_id: post.id,
            content: input.content,
            author: input.author,
            appearance: input.appearance,
        })
        .await
        .map_err(internal)?;
    emit_db_change(orch, "post_comments");
    let event = crate::orchestrator::wakes::mint_post_commented(
        orch,
        &post,
        &comment,
        input.actor_job_id.as_deref(),
    )
    .await;
    if let Err(error) = &event {
        tracing::warn!(post = post.id, %error, "post comment fact mint failed");
    }
    Ok(Published {
        record: comment,
        event,
    })
}

/// The camps the given posts address, by camp id, each read from the database
/// that owns its project (`orch`), or from `fallback` when no orchestrator is
/// at hand. One camp listing per project, however many posts.
pub(crate) async fn camps_for_posts(
    orch: Option<&Orchestrator>,
    fallback: &crate::storage::LocalDb,
    posts: &[Post],
) -> std::collections::HashMap<String, Camp> {
    let mut projects: Vec<&str> = posts
        .iter()
        .filter(|post| post.camp_id.is_some())
        .filter_map(|post| post.project_id.as_deref())
        .collect();
    projects.sort_unstable();
    projects.dedup();
    let mut camps = std::collections::HashMap::new();
    for project_id in projects {
        let owning = match orch {
            Some(orch) => crate::projects::crud::owning_db(&orch.db, project_id)
                .await
                .ok(),
            None => None,
        };
        let db = owning.as_deref().unwrap_or(fallback);
        if let Ok(listed) = db.list_camps(project_id).await {
            camps.extend(listed.into_iter().map(|camp| (camp.id.clone(), camp)));
        }
    }
    camps
}

/// Fill each camp post's display camp (`slug`, `title`) for a surface.
pub async fn attach_camps(orch: &Orchestrator, posts: &mut [Post]) {
    let camps = camps_for_posts(Some(orch), &orch.db.local, posts).await;
    for post in posts.iter_mut() {
        post.camp = post
            .camp_id
            .as_ref()
            .and_then(|id| camps.get(id))
            .map(|camp| crate::models::PostCamp {
                slug: camp.slug.clone(),
                title: camp.title.clone(),
            });
    }
}

/// The target for a post to camp `slug` of project `project_id`. Whether the
/// author may post there is [`publish_post`]'s to decide.
pub async fn camp_target(
    orch: &Orchestrator,
    project_id: &str,
    slug: &str,
) -> Result<PostTarget, PublishError> {
    let db = crate::projects::crud::owning_db(&orch.db, project_id)
        .await
        .map_err(internal)?;
    let camp = db.camp_by_slug(project_id, slug).await.map_err(internal)?;
    let camp =
        camp.ok_or_else(|| PublishError::Refused(format!("no camp `{slug}` in this project")))?;
    Ok(PostTarget::Camp {
        project_id: project_id.to_string(),
        camp_id: camp.id,
    })
}

/// The camp a post addresses, as its canonical URI, when it addresses one and
/// that camp still resolves. Built from the camp's current slug, so a fact or
/// a rendering names the camp as it is now called.
pub(crate) async fn post_camp_uri(orch: &Orchestrator, post: &Post) -> Option<String> {
    let (project_id, camp_id) = (post.project_id.as_deref()?, post.camp_id.clone()?);
    let db = crate::projects::crud::owning_db(&orch.db, project_id)
        .await
        .ok()?;
    let key = db
        .query_opt_text(
            "SELECT key FROM projects WHERE id = ?1",
            (project_id.to_string(),),
        )
        .await
        .ok()??;
    let camp = db
        .read(move |conn| {
            let camp_id = camp_id.clone();
            Box::pin(async move { cairn_db::storage::camps::camp_by_id_conn(conn, &camp_id).await })
        })
        .await
        .ok()??;
    Some(cairn_uri::build_project_camp_uri(&key, &camp.slug))
}

#[cfg(test)]
mod tests;
