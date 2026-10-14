use super::{DbError, DbResult, LocalDb, RowExt};
use crate::models::{CreatePost, CreatePostComment, Post, PostComment};
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use turso::params;

fn decode_identity(
    principal: String,
    appearance: String,
) -> DbResult<(PrincipalRef, AppearanceSnapshot)> {
    let principal = serde_json::from_str(&principal)
        .map_err(|error| DbError::internal(format!("invalid post principal: {error}")))?;
    let appearance = serde_json::from_str(&appearance)
        .map_err(|error| DbError::internal(format!("invalid post appearance: {error}")))?;
    Ok((principal, appearance))
}

/// The persisted columns of a post, in the order [`map_post`] decodes them.
pub(super) const POST_COLUMNS: &str =
    "id, project_id, title, content, author_principal_json, appearance_snapshot_json, created_at";

/// The window a reader standing in one project has on the corpus: everything
/// addressed to the whole workspace, plus that project's own scoped posts.
///
/// Written once and shared by every SQL surface that applies it — the corpus
/// read, the project timeline, and both of the feed's queries — so they cannot
/// drift apart. A function rather than a constant only because those queries
/// bind their project id at different positions; `param` is that position.
///
/// Attention routing enforces the same rule one layer up, against a job's home
/// project rather than against a row (`wakes::posts::job_may_see_post`). That is
/// an equivalence this cannot mechanically hold, so it is stated rather than
/// assumed: a change here is a change there.
pub(super) fn visible_to_project(param: u8) -> String {
    format!("(project_id IS NULL OR project_id = ?{param})")
}

/// Which posts a listing renders.
///
/// Two different questions, named rather than inferred from an `Option`: the
/// corpus as seen from somewhere, and one project's own scope. Scope is a
/// relevance tag, not an access control — it decides what an *unaddressed* read
/// of the whole corpus is worth showing, exactly as it decides which feeds,
/// timelines, and wakes a post reaches. A post addressed deliberately, by id or
/// through a named project's collection, stays readable from anywhere.
#[derive(Clone, Copy, Debug)]
pub enum PostScope<'a> {
    /// Every post in the workspace, unfiltered. The operator's own view: an
    /// operator stands in no project, so there is no other project's post to
    /// leave out.
    Corpus,
    /// The corpus as seen from inside one project, named by its row ID: the
    /// workspace-wide posts plus that project's own. What `cairn://posts`
    /// renders for an agent, and the same window
    /// [`LocalDb::list_project_post_timeline`] renders for the desktop.
    VisibleTo(&'a str),
    /// One project's own scoped posts, named by its KEY, and nothing else — the
    /// strict projection `cairn://p/<key>/posts` renders.
    Project(&'a str),
}

pub(super) fn map_post(row: &turso::Row) -> DbResult<Post> {
    let (author, appearance) = decode_identity(row.text(4)?, row.text(5)?)?;
    Ok(Post {
        id: row.i64(0)?,
        project_id: row.opt_text(1)?,
        title: row.opt_text(2)?,
        content: row.text(3)?,
        author,
        appearance,
        author_display: None,
        created_at: row.i64(6)?,
    })
}

fn map_comment(row: &turso::Row) -> DbResult<PostComment> {
    let (author, appearance) = decode_identity(row.text(3)?, row.text(4)?)?;
    Ok(PostComment {
        id: row.i64(0)?,
        post_id: row.i64(1)?,
        content: row.text(2)?,
        author,
        appearance,
        author_display: None,
        created_at: row.i64(5)?,
    })
}

impl LocalDb {
    pub async fn create_post(&self, input: CreatePost) -> DbResult<Post> {
        let principal = serde_json::to_string(&input.author)
            .map_err(|error| DbError::internal(error.to_string()))?;
        let appearance = serde_json::to_string(&input.appearance)
            .map_err(|error| DbError::internal(error.to_string()))?;
        self.write(|conn| {
            let input = input.clone();
            let principal = principal.clone();
            let appearance = appearance.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "INSERT INTO posts(project_id, title, content, author_principal_json, appearance_snapshot_json)
                     VALUES (?1, ?2, ?3, ?4, ?5)
                     RETURNING id, project_id, title, content, author_principal_json, appearance_snapshot_json, created_at",
                    params![input.project_id, input.title, input.content, principal, appearance]).await?;
                let row = rows.next().await?.ok_or_else(|| DbError::internal("created post was not returned"))?;
                map_post(&row)
            })
        }).await
    }

    pub async fn get_post(&self, id: i64) -> DbResult<Option<Post>> {
        self.query_opt("SELECT id, project_id, title, content, author_principal_json, appearance_snapshot_json, created_at FROM posts WHERE id = ?1", (id,), map_post).await
    }

    /// The newest posts one [`PostScope`] admits, optionally narrowed by a
    /// case-insensitive substring over title and content.
    ///
    /// Search is applied inside the scope, never instead of it: a scoped read
    /// with a search term cannot surface a post the same read without one would
    /// have withheld.
    pub async fn list_posts(
        &self,
        scope: PostScope<'_>,
        search: Option<&str>,
        limit: usize,
    ) -> DbResult<Vec<Post>> {
        let limit = i64::try_from(limit.min(100)).unwrap_or(100);
        let search = search.map(|value| format!("%{}%", value.to_lowercase()));
        match (scope, search) {
            (PostScope::Project(project_key), Some(search)) => self.query_all(
                "SELECT p.id, p.project_id, p.title, p.content, p.author_principal_json, p.appearance_snapshot_json, p.created_at
                 FROM posts p JOIN projects project ON project.id = p.project_id
                 WHERE project.key = ?1 AND (lower(p.title) LIKE ?2 OR lower(p.content) LIKE ?2)
                 ORDER BY p.id DESC LIMIT ?3",
                params![project_key.to_lowercase(), search, limit], map_post).await,
            (PostScope::Project(project_key), None) => self.query_all(
                "SELECT p.id, p.project_id, p.title, p.content, p.author_principal_json, p.appearance_snapshot_json, p.created_at
                 FROM posts p JOIN projects project ON project.id = p.project_id
                 WHERE project.key = ?1 ORDER BY p.id DESC LIMIT ?2",
                params![project_key.to_lowercase(), limit], map_post).await,
            (PostScope::VisibleTo(project_id), Some(search)) => self.query_all(
                format!(
                    "SELECT {POST_COLUMNS} FROM posts
                     WHERE {} AND (lower(title) LIKE ?2 OR lower(content) LIKE ?2)
                     ORDER BY id DESC LIMIT ?3",
                    visible_to_project(1)
                ),
                params![project_id.to_string(), search, limit], map_post).await,
            (PostScope::VisibleTo(project_id), None) => self.query_all(
                format!(
                    "SELECT {POST_COLUMNS} FROM posts WHERE {}
                     ORDER BY id DESC LIMIT ?2",
                    visible_to_project(1)
                ),
                params![project_id.to_string(), limit], map_post).await,
            (PostScope::Corpus, Some(search)) => self.query_all(
                format!(
                    "SELECT {POST_COLUMNS} FROM posts
                     WHERE lower(title) LIKE ?1 OR lower(content) LIKE ?1
                     ORDER BY id DESC LIMIT ?2"
                ),
                params![search, limit], map_post).await,
            (PostScope::Corpus, None) => self.query_all(
                format!("SELECT {POST_COLUMNS} FROM posts ORDER BY id DESC LIMIT ?1"),
                (limit,), map_post).await,
        }
    }

    pub async fn create_post_comment(&self, input: CreatePostComment) -> DbResult<PostComment> {
        let principal = serde_json::to_string(&input.author)
            .map_err(|error| DbError::internal(error.to_string()))?;
        let appearance = serde_json::to_string(&input.appearance)
            .map_err(|error| DbError::internal(error.to_string()))?;
        self.write(|conn| {
            let input = input.clone();
            let principal = principal.clone();
            let appearance = appearance.clone();
            Box::pin(async move {
                let mut rows = conn.query(
                    "INSERT INTO post_comments(post_id, content, author_principal_json, appearance_snapshot_json)
                     SELECT ?1, ?2, ?3, ?4 WHERE EXISTS (SELECT 1 FROM posts WHERE id = ?1)
                     RETURNING id, post_id, content, author_principal_json, appearance_snapshot_json, created_at",
                    params![input.post_id, input.content, principal, appearance]).await?;
                let row = rows.next().await?.ok_or_else(|| DbError::internal(format!("post {} does not exist", input.post_id)))?;
                map_comment(&row)
            })
        }).await
    }

    pub async fn list_post_comments(&self, post_id: i64) -> DbResult<Vec<PostComment>> {
        self.query_all("SELECT id, post_id, content, author_principal_json, appearance_snapshot_json, created_at FROM post_comments WHERE post_id = ?1 ORDER BY id", (post_id,), map_comment).await
    }

    /// The newest posts a reader standing in one project can see, and exactly
    /// those posts' comments.
    ///
    /// Visibility is the corpus rule the rest of the system already enforces —
    /// a workspace-wide post reaches every surface, a project-scoped post
    /// reaches only its own project — the same [`visible_to_project`] predicate
    /// [`PostScope::VisibleTo`] reads the corpus through and the feed pages
    /// through, and the same rule attention routing applies when it decides
    /// which subscribers a post may wake. That is a different question from
    /// [`PostScope::Project`], which is the strict projection of one project's
    /// own scope and is what `cairn://p/<key>/posts` renders.
    ///
    /// Posts and comments come back from one snapshot, and the comments are
    /// windowed to the returned posts by construction, so a caller cannot pair
    /// a page of posts with a mismatched page of comments.
    pub async fn list_project_post_timeline(
        &self,
        project_id: &str,
        limit: usize,
    ) -> DbResult<(Vec<Post>, Vec<PostComment>)> {
        let limit = i64::try_from(limit.min(200)).unwrap_or(200);
        let project_id = project_id.to_string();
        self.read(move |conn| {
            let project_id = project_id.clone();
            Box::pin(async move {
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT {POST_COLUMNS} FROM posts WHERE {}
                             ORDER BY id DESC LIMIT ?2",
                            visible_to_project(1)
                        ),
                        params![project_id.clone(), limit],
                    )
                    .await?;
                let mut posts = Vec::new();
                while let Some(row) = rows.next().await? {
                    posts.push(map_post(&row)?);
                }
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT id, post_id, content, author_principal_json, appearance_snapshot_json, created_at
                             FROM post_comments
                             WHERE post_id IN (
                                 SELECT id FROM posts WHERE {}
                                 ORDER BY id DESC LIMIT ?2
                             )
                             ORDER BY id",
                            visible_to_project(1)
                        ),
                        params![project_id, limit],
                    )
                    .await?;
                let mut comments = Vec::new();
                while let Some(row) = rows.next().await? {
                    comments.push(map_comment(&row)?);
                }
                Ok((posts, comments))
            })
        })
        .await
    }
}
