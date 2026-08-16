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

use crate::mcp::types::McpCallbackRequest;
use crate::models::Post;
use crate::orchestrator::Orchestrator;
use crate::storage::{LocalDb, PostScope};
use cairn_common::query::QueryParam;

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

/// One post, rendered the same way wherever it is read — the workspace corpus,
/// a project projection, or a home's feed.
pub(super) fn render_post(post: &Post) -> String {
    format!(
        "## [{}](cairn://posts/{})\n\n{}\n\n- Scope: {}\n- Author: {}\n- Created: {}\n",
        post.title.as_deref().unwrap_or("Untitled"),
        post.id,
        post.content,
        post.project_id.as_deref().unwrap_or("workspace"),
        serde_json::to_string(&post.author).unwrap_or_default(),
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
            format!(
                "# Posts\n\n{}",
                posts.iter().map(render_post).collect::<Vec<_>>().join("\n")
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
    let mut output = format!("# {}\n\n{}\n\n- Post: cairn://posts/{id}\n- Scope: {}\n- Author: {}\n- Created: {}\n\n## Comments\n",
        post.title.as_deref().unwrap_or("Untitled"), post.content,
        post.project_id.as_deref().unwrap_or("workspace"),
        serde_json::to_string(&post.author).unwrap_or_default(), post.created_at);
    if comments.is_empty() {
        output.push_str("\nNo comments.\n");
    }
    for comment in comments {
        output.push_str(&format!(
            "\n### Comment {}\n\n{}\n\n- Author: {}\n- Created: {}\n- Parent: cairn://posts/{id}\n",
            comment.id,
            comment.content,
            serde_json::to_string(&comment.author).unwrap_or_default(),
            comment.created_at
        ));
    }
    output
}
