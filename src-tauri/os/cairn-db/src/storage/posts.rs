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
    "id, project_id, title, content, author_principal_json, appearance_snapshot_json, created_at, camp_id";

/// [`POST_COLUMNS`] qualified by the `p` alias, for queries that join.
const POST_COLUMNS_P: &str = "p.id, p.project_id, p.title, p.content, p.author_principal_json, p.appearance_snapshot_json, p.created_at, p.camp_id";

/// The window a reader standing in one project has on the corpus: everything
/// addressed to the whole workspace, plus that project's own scoped posts.
///
/// Written once and shared by every SQL surface that applies it — the corpus
/// read, the project timeline, and both of the feed's queries — so they cannot
/// drift apart. A function rather than a constant only because those queries
/// bind their project id at different positions; `param` is that position.
///
/// The fact log's index lane enforces the same rule one layer up, against a
/// job's home project rather than against a row (`routes::predicate::
/// scope_exclusion`, which every post recipient passes). That is an equivalence
/// this cannot mechanically hold, so it is stated rather than assumed: a change
/// here is a change there.
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
    /// One camp's posts, named by the camp's immutable id — what
    /// `cairn://p/<key>/camps/<slug>/posts` renders. A camp post is also one of
    /// its project's posts, so it appears in that project's projection too.
    Camp(&'a str),
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
        camp_id: row.opt_text(7)?,
        camp: None,
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
                    format!(
                        "INSERT INTO posts(project_id, title, content, author_principal_json, appearance_snapshot_json, camp_id)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
                         RETURNING {POST_COLUMNS}"
                    ),
                    params![input.project_id, input.title, input.content, principal, appearance, input.camp_id]).await?;
                let row = rows.next().await?.ok_or_else(|| DbError::internal("created post was not returned"))?;
                map_post(&row)
            })
        }).await
    }

    pub async fn get_post(&self, id: i64) -> DbResult<Option<Post>> {
        self.query_opt(
            format!("SELECT {POST_COLUMNS} FROM posts WHERE id = ?1"),
            (id,),
            map_post,
        )
        .await
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
            (PostScope::Project(project_key), Some(search)) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS_P}
                     FROM posts p JOIN projects project ON project.id = p.project_id
                     WHERE project.key = ?1 AND (lower(p.title) LIKE ?2 OR lower(p.content) LIKE ?2)
                     ORDER BY p.id DESC LIMIT ?3"
                    ),
                    params![project_key.to_lowercase(), search, limit],
                    map_post,
                )
                .await
            }
            (PostScope::Project(project_key), None) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS_P}
                     FROM posts p JOIN projects project ON project.id = p.project_id
                     WHERE project.key = ?1 ORDER BY p.id DESC LIMIT ?2"
                    ),
                    params![project_key.to_lowercase(), limit],
                    map_post,
                )
                .await
            }
            (PostScope::Camp(camp_id), Some(search)) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS} FROM posts
                     WHERE camp_id = ?1 AND (lower(title) LIKE ?2 OR lower(content) LIKE ?2)
                     ORDER BY id DESC LIMIT ?3"
                    ),
                    params![camp_id.to_string(), search, limit],
                    map_post,
                )
                .await
            }
            (PostScope::Camp(camp_id), None) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS} FROM posts WHERE camp_id = ?1
                     ORDER BY id DESC LIMIT ?2"
                    ),
                    params![camp_id.to_string(), limit],
                    map_post,
                )
                .await
            }
            (PostScope::VisibleTo(project_id), Some(search)) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS} FROM posts
                     WHERE {} AND (lower(title) LIKE ?2 OR lower(content) LIKE ?2)
                     ORDER BY id DESC LIMIT ?3",
                        visible_to_project(1)
                    ),
                    params![project_id.to_string(), search, limit],
                    map_post,
                )
                .await
            }
            (PostScope::VisibleTo(project_id), None) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS} FROM posts WHERE {}
                     ORDER BY id DESC LIMIT ?2",
                        visible_to_project(1)
                    ),
                    params![project_id.to_string(), limit],
                    map_post,
                )
                .await
            }
            (PostScope::Corpus, Some(search)) => {
                self.query_all(
                    format!(
                        "SELECT {POST_COLUMNS} FROM posts
                     WHERE lower(title) LIKE ?1 OR lower(content) LIKE ?1
                     ORDER BY id DESC LIMIT ?2"
                    ),
                    params![search, limit],
                    map_post,
                )
                .await
            }
            (PostScope::Corpus, None) => {
                self.query_all(
                    format!("SELECT {POST_COLUMNS} FROM posts ORDER BY id DESC LIMIT ?1"),
                    (limit,),
                    map_post,
                )
                .await
            }
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

    /// How many posts each camp of one project holds, and when the newest was
    /// written, keyed by camp id. A camp with no posts is absent.
    pub async fn camp_post_stats(
        &self,
        project_id: &str,
    ) -> DbResult<std::collections::HashMap<String, (i64, i64)>> {
        Ok(self
            .query_all(
                "SELECT camp_id, COUNT(*), MAX(created_at) FROM posts
                 WHERE project_id = ?1 AND camp_id IS NOT NULL GROUP BY camp_id",
                (project_id.to_string(),),
                |row| Ok((row.text(0)?, (row.i64(1)?, row.i64(2)?))),
            )
            .await?
            .into_iter()
            .collect())
    }

    /// The newest post of each camp of one project, keyed by camp id. A camp
    /// with no posts is absent.
    pub async fn last_camp_posts(
        &self,
        project_id: &str,
    ) -> DbResult<std::collections::HashMap<String, Post>> {
        Ok(self
            .query_all(
                format!(
                    "SELECT {POST_COLUMNS} FROM posts
                     WHERE id IN (
                         SELECT MAX(id) FROM posts
                          WHERE project_id = ?1 AND camp_id IS NOT NULL
                          GROUP BY camp_id
                     )"
                ),
                (project_id.to_string(),),
                map_post,
            )
            .await?
            .into_iter()
            .filter_map(|post| post.camp_id.clone().map(|camp_id| (camp_id, post)))
            .collect())
    }

    /// Per camp of one project, how many posts by agents are newer than
    /// `reader`'s cursor. The operator's own posts carry a human or machine
    /// principal and so are never unread to the operator. A camp with nothing
    /// unread is absent.
    pub async fn camp_unread_counts(
        &self,
        reader: &str,
        project_id: &str,
    ) -> DbResult<std::collections::HashMap<String, i64>> {
        Ok(self
            .query_all(
                "SELECT p.camp_id, COUNT(*) FROM posts p
                 LEFT JOIN camp_read_cursors c ON c.camp_id = p.camp_id AND c.reader = ?1
                 WHERE p.project_id = ?2 AND p.camp_id IS NOT NULL
                   AND p.id > COALESCE(c.seen_through_post_id, 0)
                   AND p.author_principal_json LIKE '{\"kind\":\"agent\"%'
                 GROUP BY p.camp_id",
                (reader.to_string(), project_id.to_string()),
                |row| Ok((row.text(0)?, row.i64(1)?)),
            )
            .await?
            .into_iter()
            .collect())
    }

    /// Move `reader`'s cursor on a camp forward to `through_post_id`, clamped
    /// to the camp's newest post so a cursor never claims a post not yet
    /// written. A cursor only moves forward.
    pub async fn advance_camp_read_cursor(
        &self,
        reader: &str,
        camp_id: &str,
        through_post_id: i64,
        now: i64,
    ) -> DbResult<()> {
        let (reader, camp_id) = (reader.to_string(), camp_id.to_string());
        self.write(move |conn| {
            let (reader, camp_id) = (reader.clone(), camp_id.clone());
            Box::pin(async move {
                conn.execute(
                    "INSERT INTO camp_read_cursors (reader, camp_id, seen_through_post_id, seen_at)
                     SELECT ?1, ?2, MIN(?3, newest), ?4
                       FROM (SELECT MAX(id) AS newest FROM posts WHERE camp_id = ?2)
                      WHERE newest IS NOT NULL
                     ON CONFLICT (camp_id, reader) DO UPDATE SET
                       seen_through_post_id = MAX(seen_through_post_id, excluded.seen_through_post_id),
                       seen_at = excluded.seen_at",
                    params![reader, camp_id, through_post_id, now],
                )
                .await?;
                Ok(())
            })
        })
        .await
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
    ///
    /// `camp_id` narrows the window to one camp's posts, which are always
    /// inside it.
    pub async fn list_project_post_timeline(
        &self,
        project_id: &str,
        camp_id: Option<&str>,
        limit: usize,
    ) -> DbResult<(Vec<Post>, Vec<PostComment>)> {
        let limit = i64::try_from(limit.min(200)).unwrap_or(200);
        let project_id = project_id.to_string();
        let camp_id = camp_id.map(str::to_string);
        self.read(move |conn| {
            let project_id = project_id.clone();
            let camp_id = camp_id.clone();
            Box::pin(async move {
                // `?3` is NULL for the whole window, so one statement serves both.
                let window = format!("{} AND (?3 IS NULL OR camp_id = ?3)", visible_to_project(1));
                let mut rows = conn
                    .query(
                        &format!(
                            "SELECT {POST_COLUMNS} FROM posts WHERE {window}
                             ORDER BY id DESC LIMIT ?2"
                        ),
                        params![project_id.clone(), limit, camp_id.clone()],
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
                                 SELECT id FROM posts WHERE {window}
                                 ORDER BY id DESC LIMIT ?2
                             )
                             ORDER BY id"
                        ),
                        params![project_id, limit, camp_id],
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
