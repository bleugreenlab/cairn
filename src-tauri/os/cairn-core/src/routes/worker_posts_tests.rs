//! Posts on the fact log, end to end: mint at the write site, index, drain,
//! and what each recipient was handed.

use super::fixture::*;
use super::*;
use crate::models::{CreatePost, CreatePostComment, Post, PostComment};
use crate::storage::{LocalDb, RowExt};
use cairn_common::identity::{
    Address, AppearanceEvidence, AppearanceSnapshot, AppearanceTransport, PrincipalRef,
    VerificationMethod, VerificationRecord, VerificationStatus, VerificationStrength,
};

const FEED: &str = r#"{"fact":"post_created"}"#;
const WRITER_HOME: &str = "cairn://p/cairn/42/1/builder";

/// Beside the fixture's `writer` node and `ui` thread (`watcher`): a run for
/// the writer, two more threads in `cairn` (`other` is `watcher2`, `feed` is
/// `reader`), and a project `other` whose `ui` thread is `outsider`.
async fn homes(db: &LocalDb) {
    db.execute_batch(
        "INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('writer-run','writer','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('t2','p','other','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher2','t2','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher2-run','watcher2','running',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('t3','p','feed','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('reader','t3','p','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('reader-run','reader','running',1,1);
         INSERT INTO projects(id,workspace_id,name,key,repo_path,created_at,updated_at) VALUES('q','w','Other','other','/tmp/other',1,1);
         INSERT INTO threads(id,project_id,name,status,created_at,updated_at) VALUES('tq','q','ui','active',1,1);
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('outsider','tq','q','running','thread',1,1);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('outsider-run','outsider','running',1,1);",
    )
    .await
    .unwrap();
}

fn agent(node: &str) -> (PrincipalRef, AppearanceSnapshot) {
    let run = "d575806b-1a4d-4a1e-9b0e-2f3c4d5e6f70";
    let author = PrincipalRef::Agent {
        node: node.to_string(),
        run_id: Some(run.to_string()),
    };
    let verification = VerificationRecord::new(
        VerificationMethod::NodeSession,
        VerificationStatus::Verified,
        None,
        None,
        Some(run.to_string()),
        None,
        VerificationStrength::new("session-bound").unwrap(),
        900,
    )
    .unwrap();
    let evidence = AppearanceEvidence::new(
        AppearanceTransport::ResourcePatch,
        Address::Resource {
            node: node.to_string(),
        },
        verification,
        900,
        None,
    )
    .unwrap();
    let appearance = AppearanceSnapshot::new(author.clone(), evidence, vec![], None).unwrap();
    (author, appearance)
}

fn human() -> (PrincipalRef, AppearanceSnapshot) {
    let author = PrincipalRef::Human {
        issuer: "https://identity.example".to_string(),
        subject: "operator".to_string(),
        organization: None,
    };
    let verification = VerificationRecord::new(
        VerificationMethod::JwtOperator,
        VerificationStatus::Verified,
        Some("https://identity.example".to_string()),
        Some("operator".to_string()),
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
    (author, appearance)
}

/// Write a post the way the write sites do: the row, then its fact.
async fn post(
    orch: &Orchestrator,
    project: Option<&str>,
    title: &str,
    content: &str,
    (author, appearance): (PrincipalRef, AppearanceSnapshot),
    actor: Option<&str>,
) -> Post {
    let post = orch
        .db
        .local
        .create_post(CreatePost {
            project_id: project.map(str::to_owned),
            title: Some(title.to_string()),
            content: content.to_string(),
            author,
            appearance,
        })
        .await
        .unwrap();
    let event = crate::orchestrator::wakes::mint_post_created(orch, &post, actor)
        .await
        .unwrap()
        .expect("a new post mints a row");
    let id = orch
        .db
        .local
        .query_opt_i64(
            "SELECT id FROM fact_log WHERE identity = ?1",
            (format!("cairn://posts/{}", post.id),),
        )
        .await
        .unwrap()
        .unwrap();
    assert!(event.ends_with(&format!("/events/{id}")), "{event}");
    post
}

async fn comment(
    orch: &Orchestrator,
    post: &Post,
    content: &str,
    (author, appearance): (PrincipalRef, AppearanceSnapshot),
    actor: Option<&str>,
) -> PostComment {
    let comment = orch
        .db
        .local
        .create_post_comment(CreatePostComment {
            post_id: post.id,
            content: content.to_string(),
            author,
            appearance,
        })
        .await
        .unwrap();
    crate::orchestrator::wakes::mint_post_commented(orch, post, &comment, actor)
        .await
        .unwrap()
        .expect("a new comment mints a row");
    comment
}

fn post_uri(post: &Post) -> String {
    format!("cairn://posts/{}", post.id)
}

fn comment_uri(comment: &PostComment) -> String {
    format!("cairn://posts/{}/comments/{}", comment.post_id, comment.id)
}

async fn row_of(db: &LocalDb, identity: &str) -> FactRow {
    let id = db
        .query_opt_i64(
            "SELECT id FROM fact_log WHERE identity = ?1",
            (identity.to_string(),),
        )
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("no fact row for {identity}"));
    crate::storage::get_fact(db, id).await.unwrap().unwrap()
}

fn fields(row: &FactRow) -> BTreeMap<String, Value> {
    serde_json::from_str(&row.fields_json).unwrap()
}

/// Who a row woke, as `(job, via)` sorted by job: `via` names the addressee
/// field that reached a job, `None` a standing trigger.
async fn woke(db: &LocalDb, identity: &str) -> Vec<(String, Option<String>)> {
    let wakes: Vec<Value> = row_of(db, identity)
        .await
        .wakes_json
        .map(|json| serde_json::from_str(&json).unwrap())
        .unwrap_or_default();
    let mut woke: Vec<(String, Option<String>)> = wakes
        .iter()
        .map(|wake| {
            assert_eq!(wake["action"], "coalesced", "{wake}");
            (
                wake["job"].as_str().unwrap().to_owned(),
                wake["via"].as_str().map(str::to_owned),
            )
        })
        .collect();
    woke.sort();
    woke
}

fn job(name: &str, via: Option<&str>) -> (String, Option<String>) {
    (name.to_string(), via.map(str::to_owned))
}

async fn events_received(db: &LocalDb, run: &str) -> Vec<String> {
    db.query_all(
        "SELECT content FROM messages WHERE recipient_run_id = ?1 AND content LIKE '[Event]%' ORDER BY rowid",
        (run.to_string(),),
        |row| row.text(0),
    )
    .await
    .unwrap()
}

async fn delivered(db: &LocalDb, job: &str) -> Vec<(String, i64)> {
    db.query_all(
        "SELECT COALESCE(delivered_action, 'pending'), count FROM fact_log_wakes WHERE job_id = ?1 ORDER BY id",
        (job.to_string(),),
        |row| Ok((row.text(0)?, row.i64(1)?)),
    )
    .await
    .unwrap()
}

async fn push_wakes(db: &LocalDb, job: &str) -> Vec<String> {
    db.query_all(
        "SELECT wake FROM attention_pushes WHERE recipient = ?1 ORDER BY rowid",
        (job.to_string(),),
        |row| row.text(0),
    )
    .await
    .unwrap()
}

async fn watch_posts(db: &LocalDb, job: &str) {
    crate::orchestrator::wakes::subscribe(
        db,
        job,
        "fact",
        Some(FEED),
        Some(&["post_created".to_string()]),
        "agent",
    )
    .await
    .unwrap();
}

async fn mute_posts(db: &LocalDb, job: &str) {
    crate::orchestrator::wakes::mute(
        db,
        job,
        "fact",
        Some(FEED),
        Some(&["post_created".to_string()]),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
}

async fn watch_issue(db: &LocalDb, job: &str) {
    crate::orchestrator::wakes::subscribe(
        db,
        job,
        "issue",
        Some("cairn://p/cairn/42"),
        None,
        "agent",
    )
    .await
    .unwrap();
}

async fn read(orch: &Orchestrator, uri: &str) -> String {
    let request = crate::mcp::types::McpCallbackRequest {
        thread_id: None,
        cwd: "/tmp".to_string(),
        run_id: None,
        tool: "read".to_string(),
        payload: serde_json::json!({}),
        tool_use_id: None,
    };
    crate::resources::read_cairn_resource(orch, &request, uri).await
}

/// Acceptance 1. One row, one wake per watcher of the cited issue, once even
/// for a watcher that also watches Posts, and never the author's own home.
#[tokio::test]
async fn a_post_citing_an_issue_wakes_each_watcher_once_and_never_its_author() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    for watcher in ["watcher", "watcher2", "writer"] {
        watch_issue(&db, watcher).await;
    }
    watch_posts(&db, "watcher2").await;
    watch_posts(&db, "writer").await;

    let written = post(
        &orch,
        Some("p"),
        "Status",
        "Please look at cairn://p/cairn/42 before Friday.",
        agent(WRITER_HOME),
        Some("writer"),
    )
    .await;
    drain_fact_log(&orch).await;

    assert_eq!(
        db.query_opt_i64(
            "SELECT COUNT(*) FROM fact_log WHERE source = 'post_created'",
            ()
        )
        .await
        .unwrap(),
        Some(1)
    );
    let row = row_of(&db, &post_uri(&written)).await;
    assert_eq!(
        fields(&row)["mentions"],
        serde_json::json!(["cairn://p/cairn/42"])
    );
    assert_eq!(
        woke(&db, &post_uri(&written)).await,
        vec![
            job("watcher", Some("mentions")),
            job("watcher2", Some("mentions"))
        ],
        "a watcher that also watches Posts is woken once, and the author never"
    );
    assert_eq!(events_received(&db, "watcher-run").await.len(), 1);
    assert_eq!(events_received(&db, "watcher2-run").await.len(), 1);
    assert!(events_received(&db, "writer-run").await.is_empty());
}

/// Acceptance 2. Only structural references are mentions, and a qualified
/// thread name reaches the session the thread holds when the row is indexed.
#[tokio::test]
async fn bare_names_wake_no_one_and_a_qualified_thread_reaches_its_current_session() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;

    let prose = post(
        &orch,
        Some("p"),
        "Prose",
        "the ui thread should read src/lib, and cairn:// is a scheme",
        human(),
        None,
    )
    .await;
    let qualified = post(
        &orch,
        Some("p"),
        "Qualified",
        "cairn/ui should see this",
        human(),
        None,
    )
    .await;
    // The thread rotates to a new session after the mint, before delivery.
    db.execute_batch(
        "UPDATE jobs SET status = 'complete' WHERE id = 'watcher';
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher-b','t','p','running','thread',5,5);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher-b-run','watcher-b','running',5,5);",
    )
    .await
    .unwrap();
    drain_fact_log(&orch).await;

    assert_eq!(
        fields(&row_of(&db, &post_uri(&prose)).await)["mentions"],
        serde_json::json!([])
    );
    assert_eq!(
        woke(&db, &post_uri(&prose)).await,
        vec![job("reader", None)]
    );
    assert_eq!(
        fields(&row_of(&db, &post_uri(&qualified)).await)["mentions"],
        serde_json::json!(["cairn://p/cairn/ui"])
    );
    assert_eq!(
        woke(&db, &post_uri(&qualified)).await,
        vec![job("reader", None), job("watcher-b", Some("mentions"))]
    );
    assert_eq!(events_received(&db, "watcher-b-run").await.len(), 1);
    assert!(events_received(&db, "watcher-run").await.is_empty());
}

/// Acceptance 3, as the worker's rule over post rows: a project post reaches no
/// home outside its project by mention or by watch; a workspace post reaches
/// both.
#[tokio::test]
async fn a_project_post_stays_in_its_project_and_a_workspace_post_reaches_all() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    watch_posts(&db, "outsider").await;

    let text = "For cairn://p/other/ui as well.";
    let scoped = post(&orch, Some("p"), "Scoped", text, human(), None).await;
    let workspace = post(&orch, None, "Workspace", text, human(), None).await;
    drain_fact_log(&orch).await;

    assert_eq!(
        woke(&db, &post_uri(&scoped)).await,
        vec![job("reader", None)],
        "neither the mention nor the watch carries a cairn post into other"
    );
    assert_eq!(
        woke(&db, &post_uri(&workspace)).await,
        vec![job("outsider", Some("mentions")), job("reader", None)]
    );
}

/// Acceptance 4. A comment addresses its post's author and whoever it names,
/// never the Posts watchers; and a `mentions` trigger hears a thread citing the
/// post in the same vocabulary.
#[tokio::test]
async fn a_comment_reaches_the_author_and_its_mentions_and_triggers_hear_citations() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    let design = post(
        &orch,
        Some("p"),
        "Design",
        "a proposal",
        agent(WRITER_HOME),
        Some("writer"),
    )
    .await;
    let other = post(
        &orch,
        Some("p"),
        "Other",
        "elsewhere",
        agent(WRITER_HOME),
        Some("writer"),
    )
    .await;
    let design_uri = post_uri(&design);
    standing_triggers(
        &db,
        "watcher2",
        &serde_json::json!([
            {"fact": "post_commented", "postUri": design_uri},
            {"fact": "thread_stream", "mentions": design_uri}
        ])
        .to_string(),
    )
    .await;
    // The speaking thread holds the same citation trigger: its own utterance
    // must not wake it (the row's actor is the speaker).
    standing_triggers(
        &db,
        "watcher",
        &serde_json::json!([{"fact": "thread_stream", "mentions": design_uri}]).to_string(),
    )
    .await;
    drain_fact_log(&orch).await;

    let on_design = comment(&orch, &design, "cc cairn://p/cairn/ui", human(), None).await;
    let on_other = comment(&orch, &other, "unrelated", human(), None).await;
    for (n, text) in [
        (1, format!("see {design_uri}")),
        (2, "nothing cited".to_string()),
    ] {
        let detail = format!("cairn://p/cairn/ui:event:{n}");
        let fields = crate::channels::router::thread_stream_fields(
            &db,
            "cairn",
            "cairn://p/cairn/ui",
            &detail,
            text,
            "ctx".into(),
            "watcher".into(),
        )
        .await;
        crate::routes::mint(
            &orch,
            crate::routes::Mint {
                fact: crate::routes::RouteFact {
                    source: "thread_stream".into(),
                    identity: detail.clone(),
                    fields,
                    origin: None,
                    summary: None,
                    route_provenance: None,
                },
                record_ref: detail,
                record_id: Some(n.to_string()),
                project_id: Some("p"),
                presence: crate::routes::Presence::Away,
                actor_job_id: Some("watcher"),
            },
        )
        .await
        .unwrap();
    }
    drain_fact_log(&orch).await;

    let row = row_of(&db, &comment_uri(&on_design)).await;
    let stored = fields(&row);
    assert_eq!(stored["postAuthor"], WRITER_HOME);
    assert_eq!(stored["postUri"], design_uri.as_str());
    assert_eq!(
        stored["mentions"],
        serde_json::json!(["cairn://p/cairn/ui"])
    );
    assert_eq!(
        woke(&db, &comment_uri(&on_design)).await,
        vec![
            job("watcher", Some("mentions")),
            job("watcher2", None),
            job("writer", Some("postAuthor")),
        ],
        "the author, the mention, and the trigger; never the Posts watcher"
    );
    assert_eq!(
        woke(&db, &comment_uri(&on_other)).await,
        vec![job("writer", Some("postAuthor"))]
    );
    assert_eq!(
        woke(&db, "cairn://p/cairn/ui:event:1").await,
        vec![job("watcher2", None)],
        "another thread's citation of the post matches the same mentions clause; the speaker's own does not wake it"
    );
    assert!(woke(&db, "cairn://p/cairn/ui:event:2").await.is_empty());

    // The events view explains a mention wake as the row's own address.
    let explained = read(
        &orch,
        &format!(
            "cairn://p/cairn/events/{}?explain=cairn://p/cairn/ui",
            row.id
        ),
    )
    .await;
    assert!(
        explained.contains("- addressed via mentions:"),
        "{explained}"
    );
    assert!(
        !explained.contains("has no standing trigger"),
        "{explained}"
    );
    let member = read(&orch, &format!("cairn://p/cairn/events/{}", row.id)).await;
    assert!(member.contains("- post_comments.id: "), "{member}");
    assert!(member.contains("addressed via postAuthor"), "{member}");
}

/// Acceptance 5. The wake hands over the writing; the row holds only the
/// registry's stored fields, no body.
#[tokio::test]
async fn the_wake_carries_the_writing_and_the_row_holds_no_body() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    let written = post(
        &orch,
        Some("p"),
        "Wake legibility",
        "Test post to see if the wake works",
        agent(WRITER_HOME),
        Some("writer"),
    )
    .await;
    drain_fact_log(&orch).await;
    comment(&orch, &written, "A reply worth reading", human(), None).await;
    drain_fact_log(&orch).await;

    let row = row_of(&db, &post_uri(&written)).await;
    let stored = fields(&row);
    let registry = FactRegistry::default();
    let spec = registry.source("post_created").unwrap();
    assert!(stored
        .keys()
        .all(|key| spec.fields.get(key.as_str()).is_some_and(|f| f.stored)));
    assert_eq!(
        stored.keys().map(String::as_str).collect::<Vec<_>>(),
        vec!["author", "mentions", "postUri", "project", "scope", "title"]
    );
    assert_eq!(stored["author"], WRITER_HOME);
    assert_eq!(stored["scope"], "cairn");
    assert!(
        !row.fields_json.contains("Test post to see"),
        "{}",
        row.fields_json
    );
    assert_eq!(row.record_ref, post_uri(&written));
    assert_eq!(row.record_id, Some(written.id.to_string()));

    let received = events_received(&db, "reader-run").await;
    assert_eq!(received.len(), 1, "{received:?}");
    assert!(
        received[0].starts_with(&format!(
            "[Event] Wake legibility (post_created): cairn://p/cairn/events/{}",
            row.id
        )),
        "{}",
        received[0]
    );
    assert!(
        received[0].contains(&format!("## [Wake legibility]({})", post_uri(&written))),
        "{}",
        received[0]
    );
    assert!(received[0].contains("Test post to see if the wake works"));

    let replies = events_received(&db, "writer-run").await;
    assert_eq!(replies.len(), 1, "{replies:?}");
    assert!(
        replies[0].contains(&format!("On [Wake legibility]({}):", post_uri(&written))),
        "{}",
        replies[0]
    );
    assert!(
        replies[0].contains("A reply worth reading"),
        "{}",
        replies[0]
    );
}

/// Acceptance 6. The posts domain mints; it never writes attention itself.
#[test]
fn the_posts_domain_mints_and_never_pushes() {
    let source = include_str!("../orchestrator/wakes/posts.rs");
    for forbidden in ["attention_push", "push_with_fingerprint"] {
        assert!(
            !source.contains(forbidden),
            "orchestrator/wakes/posts.rs must not write attention ({forbidden})"
        );
    }
    assert!(source.contains("crate::routes::mint("));
}

/// Acceptance 7. The home feed reads `posts` and its own cursor, so posts
/// written through the log path read, re-lead, and acknowledge exactly as
/// before.
#[tokio::test]
async fn the_home_feed_reads_and_acknowledges_posts_written_through_the_log() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    let mut ids = Vec::new();
    for title in ["one", "two", "three"] {
        ids.push(post(&orch, None, title, title, human(), None).await.id);
    }
    drain_fact_log(&orch).await;

    let home = crate::resources::feed::resolve_feed_home(&db, &db, "cairn", 0, 0, "ui", None)
        .await
        .unwrap();
    let first = db.issue_feed_page(&home, 2).await.unwrap();
    let shown =
        |page: &crate::storage::FeedPage| page.posts.iter().map(|p| p.id).collect::<Vec<_>>();
    assert_eq!(shown(&first), ids[..2]);
    assert_eq!(first.remaining_unread, 1);
    let again = db.issue_feed_page(&home, 2).await.unwrap();
    assert_eq!(
        shown(&again),
        ids[..2],
        "an unacknowledged page leads again"
    );
    assert_ne!(again.token, first.token);
    assert_eq!(
        db.acknowledge_feed(&home, again.token.as_deref().unwrap())
            .await
            .unwrap(),
        crate::storage::FeedAck::Advanced {
            from: 0,
            to: ids[1]
        }
    );
    assert_eq!(
        shown(&db.issue_feed_page(&home, 2).await.unwrap()),
        ids[2..]
    );
}

/// A muted Posts watch is still told, quietly: the wake rides along with the
/// next run instead of rousing it.
#[tokio::test]
async fn a_muted_posts_watch_rides_along() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    mute_posts(&db, "reader").await;
    post(
        &orch,
        Some("p"),
        "Quiet",
        "for whoever is listening",
        human(),
        None,
    )
    .await;
    drain_fact_log(&orch).await;

    assert_eq!(
        delivered(&db, "reader").await,
        vec![("suppressed".to_string(), 1)]
    );
    assert_eq!(events_received(&db, "reader-run").await.len(), 1);
    assert_eq!(push_wakes(&db, "reader").await, vec!["passive"]);
}

/// A mention wakes a home whose Posts watch is muted, alone or when it lands
/// in a wake the muted watch already opened: the more specific reason claims
/// the recipient.
#[tokio::test]
async fn a_mention_wakes_a_home_whose_posts_watch_is_muted() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    mute_posts(&db, "reader").await;

    post(
        &orch,
        Some("p"),
        "Direct",
        "cairn://p/cairn/feed, look",
        human(),
        None,
    )
    .await;
    drain_fact_log(&orch).await;
    assert_eq!(
        delivered(&db, "reader").await,
        vec![("delivered".to_string(), 1)]
    );

    post(
        &orch,
        Some("p"),
        "Ambient",
        "nothing for anyone",
        human(),
        None,
    )
    .await;
    post(
        &orch,
        Some("p"),
        "Named",
        "cairn://p/cairn/feed again",
        human(),
        None,
    )
    .await;
    drain_fact_log(&orch).await;
    assert_eq!(
        delivered(&db, "reader").await,
        vec![("delivered".to_string(), 1), ("delivered".to_string(), 2)],
        "the mention folded into the muted watch's wake still wakes"
    );
    assert_eq!(push_wakes(&db, "reader").await, vec!["wake", "wake"]);
}

/// Two comments before a drain are one wake to the author, carrying the newer.
#[tokio::test]
async fn two_comments_in_one_window_are_one_wake_carrying_the_newer() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    let written = post(
        &orch,
        Some("p"),
        "Design",
        "a proposal",
        agent(WRITER_HOME),
        Some("writer"),
    )
    .await;
    drain_fact_log(&orch).await;
    comment(&orch, &written, "first remark", human(), None).await;
    comment(&orch, &written, "second remark", human(), None).await;
    drain_fact_log(&orch).await;

    assert_eq!(
        delivered(&db, "writer").await,
        vec![("delivered".to_string(), 2)]
    );
    let received = events_received(&db, "writer-run").await;
    assert_eq!(received.len(), 1, "{received:?}");
    assert!(
        received[0].starts_with("[Event] 2 post_commented rows in cairn"),
        "{}",
        received[0]
    );
    assert!(received[0].contains("second remark"), "{}", received[0]);
    assert!(!received[0].contains("first remark"), "{}", received[0]);
}

/// A comment has a canonical address of its own, which reads under its post.
#[tokio::test]
async fn a_comment_reads_at_its_own_uri_under_its_post() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    let written = post(&orch, None, "Design", "a proposal", human(), None).await;
    let reply = comment(&orch, &written, "a considered reply", human(), None).await;
    let rendered = read(&orch, &comment_uri(&reply)).await;
    assert!(
        rendered.starts_with(&format!("# On [Design]({})", post_uri(&written))),
        "{rendered}"
    );
    assert!(rendered.contains("a considered reply"), "{rendered}");
    let missing = read(&orch, &format!("cairn://posts/{}/comments/999", written.id)).await;
    assert!(missing.starts_with("Comment 999 not found"), "{missing}");
}

/// A post past the log's list bound still addresses its last reference, and a
/// thread's utterance still matches on its last citation: reference sets are
/// stored whole.
#[tokio::test]
async fn a_reference_past_the_list_bound_still_addresses_and_matches() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    let citations: String = (1..=70).map(|n| format!("cairn://posts/{n} ")).collect();
    let long = post(
        &orch,
        Some("p"),
        "Many",
        &format!("{citations}and finally cairn/ui"),
        human(),
        None,
    )
    .await;
    standing_triggers(
        &db,
        "watcher2",
        r#"[{"fact":"thread_stream","mentions":"cairn://posts/999"}]"#,
    )
    .await;
    let detail = "cairn://p/cairn/feed:event:1";
    let stream_fields = crate::channels::router::thread_stream_fields(
        &db,
        "cairn",
        "cairn://p/cairn/feed",
        detail,
        format!("{citations}and cairn://posts/999"),
        "ctx".into(),
        "reader".into(),
    )
    .await;
    crate::routes::mint(
        &orch,
        crate::routes::Mint {
            fact: crate::routes::RouteFact {
                source: "thread_stream".into(),
                identity: detail.into(),
                fields: stream_fields,
                origin: None,
                summary: None,
                route_provenance: None,
            },
            record_ref: detail.into(),
            record_id: Some("1".into()),
            project_id: Some("p"),
            presence: crate::routes::Presence::Away,
            actor_job_id: Some("reader"),
        },
    )
    .await
    .unwrap();
    drain_fact_log(&orch).await;

    let mentions = fields(&row_of(&db, &post_uri(&long)).await)["mentions"].clone();
    assert_eq!(mentions.as_array().unwrap().len(), 71);
    assert_eq!(mentions[70], "cairn://p/cairn/ui");
    assert_eq!(
        woke(&db, &post_uri(&long)).await,
        vec![job("watcher", Some("mentions"))]
    );
    assert_eq!(woke(&db, detail).await, vec![job("watcher2", None)]);
}

/// An explicit or migrated Posts watch is exactly the scope a
/// `{fact: post_created}` standing trigger derives. Rebuilding the trigger
/// defers to the row the node holds, its mute included, instead of colliding
/// with it; and a mute on a derived trigger survives the rebuild that
/// re-derives it.
#[tokio::test]
async fn a_posts_watch_and_the_equivalent_trigger_are_one_row_whose_mute_holds() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    mute_posts(&db, "reader").await;
    let commented = r#"{"fact":"post_commented","postUri":"cairn://posts/1"}"#;
    let triggers = format!(r#"[{{"fact":"post_created"}},{commented}]"#);
    standing_triggers(&db, "reader", &triggers).await;
    crate::orchestrator::wakes::mute(
        &db,
        "reader",
        "fact",
        Some(commented),
        Some(&["post_commented".to_string()]),
        None,
        None,
        "agent",
    )
    .await
    .unwrap();
    standing_triggers(&db, "reader", &triggers).await;

    let rows: Vec<(String, String, String)> = db
        .query_all(
            "SELECT CASE WHEN id LIKE 'derived:thread:%' THEN 'derived' ELSE 'own' END, source_ref, state
               FROM wake_subscriptions WHERE job_id = 'reader' AND source_kind = 'fact' ORDER BY source_ref",
            (),
            |row| Ok((row.text(0)?, row.text(1)?, row.text(2)?)),
        )
        .await
        .unwrap();
    assert_eq!(
        rows,
        vec![
            ("derived".into(), commented.into(), "muted".into()),
            ("own".into(), FEED.into(), "muted".into()),
        ]
    );
    post(
        &orch,
        Some("p"),
        "Quiet",
        "for whoever listens",
        human(),
        None,
    )
    .await;
    drain_fact_log(&orch).await;
    assert_eq!(
        delivered(&db, "reader").await,
        vec![("suppressed".to_string(), 1)]
    );
}

/// The author's home is excluded as it resolves when the row is indexed: a
/// thread that posts naming itself and then rotates sessions is not handed its
/// own writing on the new session, by mention or by watch.
#[tokio::test]
async fn a_rotated_author_home_is_not_woken_by_its_own_post() {
    let temp = tempfile::tempdir().unwrap();
    let (orch, db) = orchestrator(temp.path(), &[]).await;
    homes(&db).await;
    watch_posts(&db, "reader").await;
    let own = post(
        &orch,
        Some("p"),
        "Note to self",
        "cairn/ui should remember this",
        agent("cairn://p/cairn/ui"),
        Some("watcher"),
    )
    .await;
    db.execute_batch(
        "UPDATE jobs SET status = 'complete' WHERE id = 'watcher';
         INSERT INTO jobs(id,thread_id,project_id,status,uri_segment,created_at,updated_at) VALUES('watcher-b','t','p','running','thread',5,5);
         INSERT INTO runs(id,job_id,status,created_at,updated_at) VALUES('watcher-b-run','watcher-b','running',5,5);",
    )
    .await
    .unwrap();
    watch_posts(&db, "watcher-b").await;
    drain_fact_log(&orch).await;

    assert_eq!(
        fields(&row_of(&db, &post_uri(&own)).await)["author"],
        "cairn://p/cairn/ui"
    );
    assert_eq!(woke(&db, &post_uri(&own)).await, vec![job("reader", None)]);
    assert!(events_received(&db, "watcher-b-run").await.is_empty());
}
