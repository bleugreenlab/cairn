//! `cairn://posts` — the workspace post corpus, and the windows onto it.
//!
//! Scope is a relevance tag, not a confidentiality boundary. It decides what an
//! UNADDRESSED read of the whole corpus is worth showing — the same judgement
//! that decides which feeds a post lands in, which project timelines render it,
//! and which homes it may wake. It is not an ACL: a post named deliberately
//! stays readable from anywhere, exactly as another project's issues do.
//! [`read_corpus`] applies the window; [`read_posts`] with
//! [`PostScope::Project`] and [`read_post`] are the addressed surfaces, and
//! neither narrows by caller.

use std::collections::HashMap;

use crate::mcp::types::McpCallbackRequest;
use crate::models::Post;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, PostScope, RowExt};
use cairn_common::identity::display::PrincipalAliases;
use cairn_common::identity::{AppearanceSnapshot, PrincipalRef};
use cairn_common::query::QueryParam;
use cairn_common::uri::build_project_posts_uri;

fn value<'a>(params: &'a [QueryParam], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|param| param.key == key)
        .map(|param| param.value.as_str())
}

fn options(params: &[QueryParam]) -> Result<(usize, Option<&str>, bool), String> {
    for param in params {
        if !matches!(param.key.as_str(), "limit" | "search" | "format") {
            return Err(format!("Unsupported posts query parameter: {}", param.key));
        }
    }
    let limit = value(params, "limit")
        .map(str::parse::<usize>)
        .transpose()
        .map_err(|_| "posts limit must be a positive integer".to_string())?
        .unwrap_or(50);
    if limit == 0 {
        return Err("posts limit must be a positive integer".into());
    }
    let json = match value(params, "format") {
        None => false,
        Some("json") => true,
        Some(_) => return Err("posts format must be json".into()),
    };
    Ok((
        limit.min(100),
        value(params, "search").filter(|search| !search.is_empty()),
        json,
    ))
}

/// What one rendering pass resolves before it renders anything: how principals
/// are named on this installation, and what the scopes it encountered are
/// called.
///
/// Both are render-time projections of stored ids, never a stored byte — a post
/// keeps the `PrincipalRef` and the `projects` row id it was written with, and
/// `format=json` still returns exactly those. Resolved once for a whole page
/// rather than once per post, so a hundred posts read each registry once.
pub(super) struct PostContext {
    aliases: PrincipalAliases,
    /// `projects.id` → that project's key. Left empty when no post in the pass
    /// carries a scope, since there is then nothing to name.
    project_keys: HashMap<String, String>,
}

impl PostContext {
    pub(super) async fn resolve(db: &LocalDb, posts: &[Post]) -> Self {
        let project_keys = if posts.iter().any(|post| post.project_id.is_some()) {
            db.query_all("SELECT id, key FROM projects", (), |row| {
                Ok((row.text(0)?, row.text(1)?))
            })
            .await
            .unwrap_or_default()
            .into_iter()
            .collect()
        } else {
            HashMap::new()
        };
        Self {
            aliases: crate::identity::display::principal_aliases(db).await,
            project_keys,
        }
    }

    /// How a post's scope reads: the readable word for the workspace-wide
    /// corpus, or the project's key linked to that project's own collection.
    ///
    /// A scope whose project row this database cannot resolve renders as the id
    /// it actually holds — the same honesty an unresolved principal gets, since
    /// a wrong name is worse than a raw id.
    fn scope(&self, project_id: Option<&str>) -> String {
        let Some(project_id) = project_id else {
            return "workspace".to_string();
        };
        match self.project_keys.get(project_id) {
            Some(key) => format!("[{key}]({})", build_project_posts_uri(key)),
            None => project_id.to_string(),
        }
    }

    /// How an author reads on a surface with no tooltip to demote the canonical
    /// identity into: the readable label followed by the identity it stands for
    /// — for an agent, the node home URI a reader can go read. The run that
    /// wrote the row is provenance rather than identity and stays in
    /// `format=json`.
    fn author(&self, principal: &PrincipalRef, appearance: &AppearanceSnapshot) -> String {
        self.aliases
            .display(Some(principal), Some(appearance))
            .inline()
    }
}

/// One post, rendered the same way wherever it is read — the workspace corpus,
/// a project projection, or a home's feed.
pub(super) fn render_post(post: &Post, context: &PostContext) -> String {
    format!(
        "## [{}](cairn://posts/{})\n\n{}\n\n- Scope: {}\n- Author: {}\n- Created: {}\n",
        post.title.as_deref().unwrap_or("Untitled"),
        post.id,
        post.content,
        context.scope(post.project_id.as_deref()),
        context.author(&post.author, &post.appearance),
        post.created_at
    )
}

/// The project whose window a caller of `cairn://posts` reads the corpus
/// through, or `None` when the caller stands in no project at all.
///
/// An agent stands in exactly one project, so its corpus is the workspace-wide
/// posts plus its own project's — the same window `list_project_post_timeline`
/// renders for the desktop and the same rule wake routing applies at delivery.
/// A request carrying no run identity is the operator's own (the desktop app and
/// `cairn read` from a shell carry no `CAIRN_RUN_ID`); an operator holds no
/// project jurisdiction, so there is no other project's post to leave out and
/// the whole corpus is theirs.
///
/// Fail-closed on identity: a request that DOES claim a run must resolve to a
/// project or the read errors. Degrading an unresolvable agent identity to the
/// unfiltered corpus would make a broken run row indistinguishable from an
/// operator, which is precisely the direction that must never be guessed. A run
/// that resolves always carries a project — [`lookup_run_routed`] reaches its
/// project through an inner join on `jobs.project_id` — so "resolved but
/// project-less" is not a reachable state and is not special-cased into one.
///
/// [`lookup_run_routed`]: crate::mcp::handlers::run_context::lookup_run_routed
async fn corpus_window(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
) -> Result<Option<String>, String> {
    if !request
        .run_id
        .as_deref()
        .is_some_and(|run_id| !run_id.is_empty())
    {
        return Ok(None);
    }
    let (run, _) = crate::mcp::handlers::run_context::lookup_run_routed(&orch.db, request).await?;
    // A post's scope is a `projects` row in the LOCAL database — posts are
    // workspace-private and do not sync — so the id the window compares against
    // must come from the same database `create_post` stamped it from, not from
    // the (possibly team) replica that owns the run's rows. Same derivation the
    // feed uses, for the same reason.
    crate::mcp::handlers::run_context::project_id_by_key(&orch.db.local, &run.project_key)
        .await
        .map(Some)
}

/// `cairn://posts` — the corpus as the caller may see it.
pub(super) async fn read_corpus(
    orch: &Orchestrator,
    request: &McpCallbackRequest,
    params: &[QueryParam],
) -> String {
    match corpus_window(orch, request).await {
        Ok(Some(project_id)) => {
            read_posts(&orch.db.local, PostScope::VisibleTo(&project_id), params).await
        }
        Ok(None) => read_posts(&orch.db.local, PostScope::Corpus, params).await,
        Err(error) => format!(
            "Failed to read posts: this request carries a run identity Cairn cannot resolve to a \
             project, so the corpus window it may see is unknown. A read is not widened to the \
             whole workspace on an unknown window. ({error})"
        ),
    }
}

pub(super) async fn read_posts(
    db: &LocalDb,
    scope: PostScope<'_>,
    params: &[QueryParam],
) -> String {
    let (limit, search, json) = match options(params) {
        Ok(value) => value,
        Err(error) => return error,
    };
    match db.list_posts(scope, search, limit).await {
        Ok(posts) => {
            if json {
                return serde_json::to_string(&posts).unwrap_or_else(|error| error.to_string());
            }
            if posts.is_empty() {
                return "# Posts\n\nNo posts found.".into();
            }
            let context = PostContext::resolve(db, &posts).await;
            format!(
                "# Posts\n\n{}",
                posts
                    .iter()
                    .map(|post| render_post(post, &context))
                    .collect::<Vec<_>>()
                    .join("\n")
            )
        }
        Err(error) => format!("Failed to read posts: {error}"),
    }
}

pub(super) async fn read_post(db: &LocalDb, id: i64, params: &[QueryParam]) -> String {
    let (_, search, json) = match options(params) {
        Ok(value) => value,
        Err(error) => return error,
    };
    if search.is_some() {
        return "search is only supported on post collections".into();
    }
    let post = match db.get_post(id).await {
        Ok(Some(post)) => post,
        Ok(None) => return format!("Post {id} not found."),
        Err(error) => return format!("Failed to read post: {error}"),
    };
    let comments = match db.list_post_comments(id).await {
        Ok(comments) => comments,
        Err(error) => return format!("Failed to read post comments: {error}"),
    };
    if json {
        return serde_json::json!({"post": post, "comments": comments}).to_string();
    }
    let context = PostContext::resolve(db, std::slice::from_ref(&post)).await;
    let mut output = format!("# {}\n\n{}\n\n- Post: cairn://posts/{id}\n- Scope: {}\n- Author: {}\n- Created: {}\n\n## Comments\n",
        post.title.as_deref().unwrap_or("Untitled"), post.content,
        context.scope(post.project_id.as_deref()),
        context.author(&post.author, &post.appearance), post.created_at);
    if comments.is_empty() {
        output.push_str("\nNo comments.\n");
    }
    for comment in comments {
        output.push_str(&format!(
            "\n### Comment {}\n\n{}\n\n- Author: {}\n- Created: {}\n- Parent: cairn://posts/{id}\n",
            comment.id,
            comment.content,
            context.author(&comment.author, &comment.appearance),
            comment.created_at
        ));
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::{CreatePost, CreatePostComment};
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceTransport, VerificationMethod, VerificationRecord,
        VerificationStatus, VerificationStrength,
    };

    const NODE: &str = "cairn://p/cairn/4198/1/builder";
    const RUN: &str = "d575806b-1a4d-4a1e-9b0e-2f3c4d5e6f70";
    /// A project row id, in the shape the scope column actually holds.
    const PROJECT: &str = "00ace0d0-24a5-4700-83ba-cc719c63f43c";

    /// The principal and appearance an agent's own post is stamped with — the
    /// same shape `mutations::dispatch::posts` mints from a live run.
    fn agent() -> (PrincipalRef, AppearanceSnapshot) {
        let author = PrincipalRef::Agent {
            node: NODE.to_string(),
            run_id: Some(RUN.to_string()),
        };
        let verification = VerificationRecord::new(
            VerificationMethod::NodeSession,
            VerificationStatus::Verified,
            None,
            None,
            Some(RUN.to_string()),
            None,
            VerificationStrength::new("session-bound").unwrap(),
            900,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::ResourcePatch,
            Address::Resource {
                node: NODE.to_string(),
            },
            verification,
            900,
            None,
        )
        .unwrap();
        let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None).unwrap();
        (author, appearance)
    }

    async fn fixture() -> LocalDb {
        let db = crate::storage::migrated_test_db("posts-rendering.db").await;
        db.execute_script(&format!(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('{PROJECT}', 'default', 'Cairn', 'cairn', '/tmp/cairn', 1, 1);"
        ))
        .await
        .unwrap();
        db
    }

    async fn post(db: &LocalDb, project_id: Option<&str>, title: &str) -> i64 {
        let (author, appearance) = agent();
        db.create_post(CreatePost {
            project_id: project_id.map(str::to_string),
            title: Some(title.to_string()),
            content: format!("{title} body"),
            author,
            appearance,
        })
        .await
        .unwrap()
        .id
    }

    /// The two stored internals a post carries — the `projects` row id it is
    /// scoped to and the `PrincipalRef` it is attributed to — are ids, and a
    /// person reads neither. The default rendering resolves both: the scope to
    /// the project key, linked to that project's own collection, and the author
    /// to the home a reader can go read.
    #[tokio::test]
    async fn a_scoped_post_renders_its_project_key_and_its_author_s_home() {
        let db = fixture().await;
        post(&db, Some(PROJECT), "Scoped").await;

        let rendered = read_posts(&db, PostScope::Corpus, &[]).await;
        assert!(
            rendered.contains("- Scope: [cairn](cairn://p/cairn/posts)"),
            "{rendered}"
        );
        assert!(
            rendered.contains(&format!("- Author: cairn/4198 / builder ({NODE})")),
            "{rendered}"
        );
        assert!(
            !rendered.contains(PROJECT),
            "a resolved scope must not also print its row id: {rendered}"
        );
        assert!(
            !rendered.contains(RUN) && !rendered.contains("\"kind\""),
            "the run that wrote a post is provenance, not identity: {rendered}"
        );
    }

    /// A workspace-wide post has no project to name, and says so in the word a
    /// person would use.
    #[tokio::test]
    async fn a_workspace_post_says_workspace() {
        let db = fixture().await;
        post(&db, None, "Everyone").await;

        let rendered = read_posts(&db, PostScope::Corpus, &[]).await;
        assert!(rendered.contains("- Scope: workspace"), "{rendered}");
    }

    /// A scope this pass could not resolve renders as the id it actually holds.
    ///
    /// `posts.project_id` references `projects` ON DELETE RESTRICT, so a stored
    /// scope always has a live row and this is not reachable by deleting one; it
    /// is what a FAILED registry read degrades to, which is why the rule is
    /// asserted directly. The same honesty an unresolved principal gets: a wrong
    /// name is worse than a raw id.
    #[test]
    fn an_unresolved_scope_renders_as_itself() {
        let context = PostContext {
            aliases: PrincipalAliases::default(),
            project_keys: HashMap::new(),
        };
        assert_eq!(context.scope(Some(PROJECT)), PROJECT);
        assert_eq!(context.scope(None), "workspace");
    }

    /// One post and its comments resolve the same way the collection does, and
    /// `format=json` stays the lossless projection of what is persisted — the
    /// run id and the row-id scope included.
    #[tokio::test]
    async fn a_single_post_resolves_its_comments_while_json_stays_lossless() {
        let db = fixture().await;
        let id = post(&db, Some(PROJECT), "Scoped").await;
        let (author, appearance) = agent();
        db.create_post_comment(CreatePostComment {
            post_id: id,
            content: "a reply".to_string(),
            author,
            appearance,
        })
        .await
        .unwrap();

        let rendered = read_post(&db, id, &[]).await;
        assert_eq!(
            rendered
                .matches(&format!("- Author: cairn/4198 / builder ({NODE})"))
                .count(),
            2,
            "the post and its comment both resolve their author: {rendered}"
        );
        assert!(
            rendered.contains("- Scope: [cairn](cairn://p/cairn/posts)"),
            "{rendered}"
        );
        assert!(!rendered.contains(RUN), "{rendered}");

        let params = vec![QueryParam {
            key: "format".into(),
            value: "json".into(),
        }];
        let json: serde_json::Value =
            serde_json::from_str(&read_post(&db, id, &params).await).unwrap();
        assert_eq!(json["post"]["author"]["run_id"], RUN);
        assert_eq!(json["post"]["projectId"], PROJECT);
        assert_eq!(json["comments"][0]["author"]["run_id"], RUN);
    }
}
