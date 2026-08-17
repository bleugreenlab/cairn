//! `cairn:~/feed` — the unread post projection of one durable home.
//!
//! Two things are answered here and nowhere else: WHICH cursor a feed URI
//! addresses, and what a read of it renders. How a position moves lives entirely
//! in the storage layer, so nothing in this module can advance one.

use crate::orchestrator::Orchestrator;
use crate::storage::{FeedHome, FeedHomeKind, FeedPage, LocalDb, FEED_PAGE_DEFAULT, FEED_PAGE_MAX};
use cairn_common::query::QueryParam;
use cairn_common::uri::{build_home_feed_uri, NodeAddress};

/// Resolve the durable home a feed coordinate addresses.
///
/// A thread SESSION resolves to the thread ROW, so the position survives session
/// replacement, compaction, and rename — the name is only how a URI spells the
/// home, never what the cursor is keyed by. Everything else resolves to its own
/// job, which is what gives each sub-agent task a cursor distinct from its
/// parent's and from its siblings'.
///
/// Fail-closed: an unresolvable home is an error, never a cursor keyed on a
/// guess. Read-only throughout — acknowledging a feed must not bring a thread's
/// session job into existence.
/// `local` holds the posts and the cursors; `routed` holds the threads and jobs
/// the coordinate is resolved against. For a workspace-local project they are
/// the same handle, and for a team project they are not — which is exactly why
/// each side is named rather than derived here.
pub(crate) async fn resolve_feed_home(
    local: &LocalDb,
    routed: &LocalDb,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
) -> Result<FeedHome, String> {
    // A post's scope is a `projects` row in the LOCAL database — posts are
    // workspace-private and do not sync — so the id a feed filters on must come
    // from the same database `create_post` stamped it from.
    let project_id = crate::mcp::handlers::run_context::project_id_by_key(local, project).await?;
    let (kind, id) = match (NodeAddress::new(number, exec_seq, node_id), task_name) {
        (NodeAddress::Thread { name }, None) => {
            let thread_id = crate::threads::thread_id_by_name(routed, project, name)
                .await
                .map_err(|error| format!("Failed to resolve thread '{name}': {error}"))?
                .ok_or_else(|| format!("Thread '{name}' not found in {project}"))?;
            (FeedHomeKind::Thread, thread_id)
        }
        _ => {
            let job_id = super::node::resolve_node_or_task_job_id_for_read(
                routed, project, number, exec_seq, node_id, task_name,
            )
            .await?;
            (FeedHomeKind::Job, job_id)
        }
    };
    Ok(FeedHome {
        kind,
        id,
        project_id,
    })
}

/// `limit` and `format` — the established keys, and the only ones. A feed needs
/// no key of its own: a caller who could name a position could skip a post.
fn options(params: &[QueryParam]) -> Result<(usize, bool), String> {
    for param in params {
        if !matches!(param.key.as_str(), "limit" | "format") {
            return Err(format!(
                "Unsupported feed query parameter: {}. The feed accepts limit and format only; \
                 a reading position is server-owned and cannot be requested.",
                param.key
            ));
        }
    }
    let value = |key: &str| {
        params
            .iter()
            .find(|param| param.key == key)
            .map(|param| param.value.as_str())
    };
    let limit = value("limit")
        .map(str::parse::<usize>)
        .transpose()
        .map_err(|_| "feed limit must be a positive integer".to_string())?
        .unwrap_or(FEED_PAGE_DEFAULT);
    if limit == 0 {
        return Err("feed limit must be a positive integer".into());
    }
    let json = match value("format") {
        None => false,
        Some("json") => true,
        Some(_) => return Err("feed format must be json".into()),
    };
    Ok((limit.min(FEED_PAGE_MAX), json))
}

/// The acknowledgement instruction, carrying the token this page was issued
/// under. Written at the address that was read, so acknowledging is a copy.
fn ack_instruction(uri: &str, token: &str) -> String {
    format!(
        "Acknowledge exactly these posts:\n\n```\nwrite({{changes:[{{target:\"{uri}\",mode:\"patch\",payload:{{ack:\"{token}\"}}}}]}})\n```\n\nUntil that token is acknowledged, the next read returns these same posts under a fresh token.\n"
    )
}

/// A feed page renders its posts exactly as every other posts surface does, so
/// it resolves the same rendering context first rather than printing stored ids.
async fn render_markdown(db: &LocalDb, uri: &str, page: &FeedPage) -> String {
    let Some(token) = page.token.as_deref() else {
        return format!("# Feed\n\n`{uri}`\n\nNo unread posts. Nothing to acknowledge.\n");
    };
    let context = super::posts::PostContext::resolve(db, &page.posts).await;
    format!(
        "# Feed\n\n`{uri}`\n\n{} unread post(s), oldest first; {} more unread behind this page.\n\n{}\n{}",
        page.posts.len(),
        page.remaining_unread,
        page.posts
            .iter()
            .map(|post| super::posts::render_post(post, &context))
            .collect::<Vec<_>>()
            .join("\n"),
        ack_instruction(uri, token),
    )
}

fn render_json(uri: &str, page: &FeedPage) -> String {
    serde_json::json!({
        "uri": uri,
        "posts": page.posts,
        "acknowledged_through": page.acknowledged_through,
        "remaining_unread": page.remaining_unread,
        "ack": page.token,
    })
    .to_string()
}

pub(super) async fn read_home_feed(
    orch: &Orchestrator,
    project: &str,
    number: i32,
    exec_seq: i32,
    node_id: &str,
    task_name: Option<&str>,
    params: &[QueryParam],
) -> String {
    let (limit, json) = match options(params) {
        Ok(options) => options,
        Err(error) => return error,
    };
    let routed = orch.db.for_project(project).await;
    let home = match resolve_feed_home(
        &orch.db.local,
        &routed,
        project,
        number,
        exec_seq,
        node_id,
        task_name,
    )
    .await
    {
        Ok(home) => home,
        Err(error) => return error,
    };
    // Fail closed: a page whose issuance did not commit is never rendered, so a
    // token an agent can see always has a server record of what it showed.
    let page = match orch.db.local.issue_feed_page(&home, limit).await {
        Ok(page) => page,
        Err(error) => return format!("Failed to read feed: {error}"),
    };
    let uri = build_home_feed_uri(project, number, exec_seq, node_id, task_name);
    if json {
        render_json(&uri, &page)
    } else {
        render_markdown(&orch.db.local, &uri, &page).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::CreatePost;
    use cairn_common::identity::{
        Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
        VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
    };

    /// A project holding one thread, one issue node, and a sub-agent task under
    /// that node — the three coordinates a feed can be addressed at.
    async fn fixture() -> LocalDb {
        let db = crate::storage::migrated_test_db("feed-homes.db").await;
        db.execute_script(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-feed', 'default', 'Feed', 'fed', '/tmp/feed', 1, 1);
             INSERT INTO threads (id, project_id, name, jurisdiction, created_at, updated_at)
             VALUES ('t-design', 'p-feed', 'design-review', 'Own architecture decisions', 2, 3);
             INSERT INTO issues (id, project_id, number, title, created_at, updated_at)
             VALUES ('i-1', 'p-feed', 7, 'An issue', 4, 4);
             INSERT INTO executions (id, recipe_id, issue_id, project_id, status, started_at, seq)
             VALUES ('e-1', 'recipe-default', 'i-1', 'p-feed', 'running', 4, 1);
             INSERT INTO jobs (id, execution_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('j-builder', 'e-1', 'i-1', 'p-feed', 'builder', 'builder', 'running', 5, 5);
             INSERT INTO jobs (id, execution_id, parent_job_id, issue_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('j-probe', 'e-1', 'j-builder', 'i-1', 'p-feed', 'probe', 'probe', 'running', 6, 6);",
        )
        .await
        .unwrap();
        db
    }

    async fn post(db: &LocalDb, project_id: Option<&str>, content: &str) -> i64 {
        let author = PrincipalRef::Human {
            issuer: "https://identity.example".to_string(),
            subject: "author".to_string(),
            organization: None,
        };
        let verification = VerificationRecord::new(
            VerificationMethod::JwtOperator,
            VerificationStatus::Verified,
            Some("https://identity.example".to_string()),
            Some("author".to_string()),
            None,
            None,
            VerificationStrength::new("strong").unwrap(),
            900,
        )
        .unwrap();
        let evidence = AppearanceEvidence::new(
            AppearanceTransport::AuthenticatedOperator,
            Address::Invoke { origin: None },
            verification,
            900,
            None,
        )
        .unwrap();
        let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None).unwrap();
        db.create_post(CreatePost {
            project_id: project_id.map(str::to_string),
            title: Some("A post".to_string()),
            content: content.to_string(),
            author,
            appearance,
        })
        .await
        .unwrap()
        .id
    }

    async fn home(db: &LocalDb, node_id: &str, task_name: Option<&str>) -> FeedHome {
        let (number, exec_seq) = if node_id == "design-review" {
            (0, 0)
        } else {
            (7, 1)
        };
        resolve_feed_home(db, db, "fed", number, exec_seq, node_id, task_name)
            .await
            .unwrap()
    }

    /// A thread's cursor is keyed by its ROW, so replacing the session job and
    /// renaming the thread both leave the reading position — and an outstanding
    /// token — exactly where they were.
    #[tokio::test]
    async fn a_thread_s_cursor_survives_session_rotation_and_rename() {
        let db = fixture().await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;

        let before = home(&db, "design-review", None).await;
        assert_eq!(before.kind, FeedHomeKind::Thread);
        assert_eq!(before.id, "t-design");

        // Read a page, then rotate the session out from under it without
        // acknowledging: the token was issued to the thread, not the session.
        let page = db.issue_feed_page(&before, 10).await.unwrap();
        let token = page.token.clone().unwrap();
        crate::threads::ensure_thread_session(&db, "t-design")
            .await
            .unwrap();
        db.execute_script(
            "UPDATE jobs SET status = 'complete' WHERE thread_id = 't-design';
             INSERT INTO jobs (id, thread_id, project_id, node_name, uri_segment, status, created_at, updated_at)
             VALUES ('j-session-2', 't-design', 'p-feed', 'thread', 'thread', 'running', 9, 9);
             UPDATE threads SET name = 'design' WHERE id = 't-design';",
        )
        .await
        .unwrap();

        // The renamed thread, reached at its new address, is the same home.
        let after = resolve_feed_home(&db, &db, "fed", 0, 0, "design", None)
            .await
            .unwrap();
        assert_eq!(after, before);
        assert_eq!(
            db.acknowledge_feed(&after, &token).await.unwrap(),
            crate::storage::FeedAck::Advanced { from: 0, to: 2 },
            "a token outlives the session that read it"
        );
        assert!(db
            .issue_feed_page(&after, 10)
            .await
            .unwrap()
            .posts
            .is_empty());
    }

    /// A node, a task beneath it, and a thread are three homes with three
    /// cursors. Acknowledging one moves only that one.
    #[tokio::test]
    async fn a_node_its_task_and_a_thread_are_three_distinct_homes() {
        let db = fixture().await;
        post(&db, None, "first").await;

        let thread = home(&db, "design-review", None).await;
        let node = home(&db, "builder", None).await;
        let task = home(&db, "builder", Some("probe")).await;
        assert_eq!(
            (node.kind, node.id.as_str()),
            (FeedHomeKind::Job, "j-builder")
        );
        assert_eq!(
            (task.kind, task.id.as_str()),
            (FeedHomeKind::Job, "j-probe")
        );
        assert_ne!(thread.id, node.id);
        assert_ne!(node.id, task.id);

        let page = db.issue_feed_page(&node, 10).await.unwrap();
        db.acknowledge_feed(&node, page.token.as_deref().unwrap())
            .await
            .unwrap();
        assert!(db
            .issue_feed_page(&node, 10)
            .await
            .unwrap()
            .posts
            .is_empty());
        for other in [&thread, &task] {
            assert_eq!(
                db.issue_feed_page(other, 10).await.unwrap().posts.len(),
                1,
                "only the addressed home advances"
            );
        }
    }

    /// Every home resolves to the project it lives in, which is what makes the
    /// scope filter mean anything: a post scoped elsewhere is never its unread.
    #[tokio::test]
    async fn a_home_carries_the_project_whose_posts_it_may_see() {
        let db = fixture().await;
        db.execute(
            "INSERT INTO projects (id, workspace_id, name, key, repo_path, created_at, updated_at)
             VALUES ('p-other', 'default', 'Other', 'oth', '/tmp/other', 1, 1)",
            (),
        )
        .await
        .unwrap();
        let workspace = post(&db, None, "everyone").await;
        let own = post(&db, Some("p-feed"), "ours").await;
        post(&db, Some("p-other"), "theirs").await;

        let thread = home(&db, "design-review", None).await;
        assert_eq!(thread.project_id, "p-feed");
        let page = db.issue_feed_page(&thread, 10).await.unwrap();
        assert_eq!(
            page.posts.iter().map(|post| post.id).collect::<Vec<_>>(),
            vec![workspace, own]
        );
    }

    /// An unresolvable home is an error, never a cursor keyed on a guess.
    #[tokio::test]
    async fn an_unresolvable_home_fails_closed() {
        let db = fixture().await;
        assert!(
            resolve_feed_home(&db, &db, "fed", 0, 0, "no-such-thread", None)
                .await
                .unwrap_err()
                .contains("not found")
        );
        assert!(
            resolve_feed_home(&db, &db, "nope", 0, 0, "design-review", None)
                .await
                .is_err()
        );
    }

    /// The rendered page is what an agent acts on: canonical post links, the
    /// backlog it has not seen, and one unambiguous acknowledgement carrying the
    /// token this read issued.
    #[tokio::test]
    async fn the_markdown_page_carries_post_links_and_its_own_ack_token() {
        let db = fixture().await;
        post(&db, None, "first").await;
        post(&db, None, "second").await;
        let thread = home(&db, "design-review", None).await;

        let page = db.issue_feed_page(&thread, 1).await.unwrap();
        let uri = "cairn://p/fed/design-review/feed";
        let rendered = render_markdown(&db, uri, &page).await;
        let token = page.token.clone().unwrap();
        assert!(rendered.contains("cairn://posts/1"), "{rendered}");
        assert!(!rendered.contains("cairn://posts/2"), "{rendered}");
        assert!(rendered.contains("1 more unread"), "{rendered}");
        assert!(rendered.contains(&format!("ack:\"{token}\"")), "{rendered}");
        assert!(rendered.contains(uri), "{rendered}");

        let json: serde_json::Value = serde_json::from_str(&render_json(uri, &page)).unwrap();
        assert_eq!(json["ack"], serde_json::json!(token));
        assert_eq!(json["remaining_unread"], serde_json::json!(1));
        assert_eq!(json["acknowledged_through"], serde_json::json!(0));
        assert_eq!(json["posts"][0]["content"], serde_json::json!("first"));
    }

    /// An empty feed says so and offers nothing to acknowledge — no token, no
    /// instruction that would advertise an advancement covering no post.
    #[tokio::test]
    async fn an_empty_page_offers_no_acknowledgement() {
        let db = fixture().await;
        let thread = home(&db, "design-review", None).await;
        let page = db.issue_feed_page(&thread, 10).await.unwrap();
        let rendered = render_markdown(&db, "cairn://p/fed/design-review/feed", &page).await;
        assert!(rendered.contains("No unread posts"), "{rendered}");
        assert!(!rendered.contains("ack:"), "{rendered}");
    }

    /// A read advertises what to do next from the contract table, so the
    /// acknowledgement an agent copies out of the block is the one the mutation
    /// gate accepts.
    #[test]
    fn the_feed_advertises_its_acknowledgement_on_read() {
        let block = super::super::common::affordance_for_kind(
            cairn_common::contract::ResourceKind::HomeFeed,
        );
        assert!(block.contains("Home feed"), "{block}");
        assert!(block.contains("cairn:~/feed"), "{block}");
        assert!(block.contains("ack"), "{block}");
        assert!(block.contains("limit"), "{block}");
    }

    /// `limit` and `format` are the whole query surface. A key that could name a
    /// position is refused rather than ignored.
    #[test]
    fn the_feed_accepts_only_the_established_query_keys() {
        fn param(key: &str, value: &str) -> cairn_common::query::QueryParam {
            cairn_common::query::QueryParam {
                key: key.into(),
                value: value.into(),
            }
        }
        assert_eq!(options(&[]).unwrap(), (FEED_PAGE_DEFAULT, false));
        assert_eq!(
            options(&[param("limit", "5"), param("format", "json")]).unwrap(),
            (5, true)
        );
        assert_eq!(
            options(&[param("limit", "10000")]).unwrap(),
            (FEED_PAGE_MAX, false)
        );
        assert!(options(&[param("limit", "0")]).is_err());
        assert!(options(&[param("format", "yaml")]).is_err());
        for forbidden in ["after", "since", "position", "cursor"] {
            assert!(
                options(&[param(forbidden, "12")]).is_err(),
                "{forbidden} must not be accepted"
            );
        }
    }
}
